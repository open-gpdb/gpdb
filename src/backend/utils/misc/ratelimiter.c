#include "postgres.h"

#include "utils/ratelimiter.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"
#include "lib/ilist.h"
#include "storage/lwlock.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "utils/elog.h"
#include "miscadmin.h"
#include "utils/guc.h"

typedef struct RateLimiterShmem
{
	LWLock	   *lock;
	int			ring_capacity;
	int			ring_size;
	int			head;
	int			count;
	long		time_frame;
	TimestampTz *ring;
	PROC_QUEUE	waiters;
}	RateLimiterShmem;

Size
RateLimiterShmemSize(void)
{
	Size		sz = sizeof(RateLimiterShmem);

	add_size(sz, mul_size(sizeof(TimestampTz), 10));
	add_size(sz, sz / 10);
	return MAXALIGN(sz);
}

void *
RateLimiterShmemInit(RateLimiterDesc limiter_desc)
{
	bool		found = false;
	RateLimiterShmem *rate_limiter;

	rate_limiter = ShmemInitStruct(limiter_desc.limiter_name,
								   RateLimiterShmemSize(),
								   &found);

	if (!found)
	{
		/* Initialize new instance */
		MemSet(rate_limiter, 0, RateLimiterShmemSize());
		rate_limiter->lock = LWLockAssign();
		rate_limiter->count = 0;
		rate_limiter->ring_capacity = 1024;
		rate_limiter->ring_size = limiter_desc.num_elements;
		rate_limiter->time_frame = limiter_desc.time_frame;
		/* seconds */
		rate_limiter->ring = ShmemAlloc(sizeof(TimestampTz) * rate_limiter->ring_capacity);
		ProcQueueInit(&rate_limiter->waiters);
	}

	DefineCustomIntVariable(limiter_desc.num_elements_guc,
							limiter_desc.num_elements_guc_description,
							NULL,
							&rate_limiter->ring_size,
							rate_limiter->ring_size,
							1,
							1024,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable(limiter_desc.time_frame_guc,
							limiter_desc.time_frame_guc_description,
							NULL,
							&rate_limiter->time_frame,
							rate_limiter->time_frame,
							0,
							3600,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	return rate_limiter;
}

static void
ring_push_unlocked(RateLimiterShmem * rate_limiter, TimestampTz now)
{
	if (rate_limiter->ring_size == 0)
		return;

	if (rate_limiter->count < rate_limiter->ring_size)
	{
		int			idx = (rate_limiter->head + rate_limiter->count) % rate_limiter->ring_capacity;

		rate_limiter->ring[idx] = now;
		rate_limiter->count++;
	}
	else
	{
		rate_limiter->ring[rate_limiter->head] = now;
		rate_limiter->head++;
		rate_limiter->head %= rate_limiter->ring_capacity;
	}
}

static long
get_sleep_time_unlocked(RateLimiterShmem * rate_limiter, TimestampTz now)
{
	if (rate_limiter->ring_size == 0)
		return 0;

	if (rate_limiter->count < rate_limiter->ring_size)
		return 0;

	TimestampTz oldest = rate_limiter->ring[rate_limiter->head];
	long		elapsed = (long)(now - oldest);
	long		frame_usec = rate_limiter->time_frame * 1000000l;

	if (elapsed >= frame_usec)
		return 0;
	return frame_usec - elapsed;
}

static void
dequeue_unlocked(RateLimiterShmem * rate_limiter)
{
	PROC_QUEUE *waitQueue;
	PGPROC	   *proc;
	PGPROC	   *next;

	if (rate_limiter->count != 0)
	{
		waitQueue = &rate_limiter->waiters;
		proc = (PGPROC *)waitQueue->links.next;
		SHMQueueDelete(&proc->links);
		Assert(waitQueue->size != 0);
		waitQueue->size--;
		if (waitQueue->size != 0)
		{
			next = (PGPROC *)waitQueue->links.next;
			SetLatch(&next->procLatch);
		}
	}
}

static void
enqueue_unlocked(RateLimiterShmem * rate_limiter)
{
	PROC_QUEUE *waitQueue;
	PGPROC	   *headProc;

	waitQueue = &rate_limiter->waiters;
	headProc = (PGPROC *)&waitQueue->links;

	SHMQueueInsertBefore(&headProc->links, &(MyProc->links));

	waitQueue->size++;
}

void
RateLimit(void *limiter)
{
	bool		enqueued = false;
	int			latchRes;
	RateLimiterShmem *rate_limiter = (RateLimiterShmem *) limiter;

	while (true)
	{
		long		wait_us = 0;
		long		msec = -1;
		TimestampTz now;
		PROC_QUEUE *waitQueue;
		PGPROC	   *headProc;

		ResetLatch(&MyProc->procLatch);
		CHECK_FOR_INTERRUPTS();

		now = GetCurrentTimestamp();

		LWLockAcquire(rate_limiter->lock, LW_EXCLUSIVE);
		waitQueue = &rate_limiter->waiters;
		headProc = (PGPROC *)&waitQueue->links;

		/* Disabled? Admit immediately. */
		if (rate_limiter->ring_size == 0)
		{
			LWLockRelease(rate_limiter->lock);
			return;
		}

		/*
		 * Enqueue once at tail to preserve FIFO. Reuse PGPROC->links while
		 * waiting.
		 */
		if (!enqueued)
		{
			enqueue_unlocked(rate_limiter);
			enqueued = true;
		}

		/*
		 * Are we the head of the FIFO? Only the head is allowed to evaluate
		 * admission.
		 */
		if (headProc != (PGPROC *)&MyProc->links)
		{
			LWLockRelease(rate_limiter->lock);

			/* Sleep until someone wakes us (head admits or config change) */
			latchRes = WaitLatch(&MyProc->procLatch,
								 WL_LATCH_SET | WL_POSTMASTER_DEATH, -1);
			if (latchRes & WL_POSTMASTER_DEATH)
				elog(ERROR,
					 "got WL_POSTMASTER_DEATH waiting on latch; exiting...");
			continue;
		}

		/* We are head: check admission. */
		wait_us = get_sleep_time_unlocked(rate_limiter, now);
		if (wait_us == 0)
		{
			/* Admit now, record timestamp, dequeue self, wake next. */
			ring_push_unlocked(rate_limiter, now);
			dequeue_unlocked(rate_limiter);
			LWLockRelease(rate_limiter->lock);
			return;
		}

		/* Not yet: remain head, compute timeout and sleep. */
		/* Convert to msec; clamp to reasonable bounds. */
		msec = (wait_us + 999) / 1000;
		if (msec < 1)
			msec = 1;
		if (msec > 60000)
			msec = 60000;

		LWLockRelease(rate_limiter->lock);

		latchRes = WaitLatch(&MyProc->procLatch,
					  WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT, msec);
		if (latchRes & WL_POSTMASTER_DEATH)
			elog(ERROR,
				 "got WL_POSTMASTER_DEATH waiting on latch; exiting...");

		/*
		 * Loop to re-evaluate; still head unless config change reordered
		 * queue
		 */
	}
}
