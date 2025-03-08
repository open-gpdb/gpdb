/*-------------------------------------------------------------------------
 *
 * blscan.c
 *		Bloom index scan functions.
 *
 * Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/bloom/blscan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relscan.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "bloom.h"

/*
 * Begin scan of bloom index.
 */
Datum
blbeginscan(PG_FUNCTION_ARGS)
{
	Relation	r = (Relation) PG_GETARG_POINTER(0);
	int			nkeys = PG_GETARG_INT32(1);
	int			norderbys = PG_GETARG_INT32(2);
	IndexScanDesc scan;
	BloomScanOpaque so;

	scan = RelationGetIndexScan(r, nkeys, norderbys);

	so = (BloomScanOpaque) palloc(sizeof(BloomScanOpaqueData));
	initBloomState(&so->state, scan->indexRelation);
	so->sign = NULL;

	scan->opaque = so;

	PG_RETURN_POINTER(scan);
}

/*
 * Rescan a bloom index.
 */
Datum
blrescan(PG_FUNCTION_ARGS)
{
	IndexScanDesc	scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	ScanKey			scankey = (ScanKey) PG_GETARG_POINTER(1);
	BloomScanOpaque so = (BloomScanOpaque) scan->opaque;

	if (so->sign)
		pfree(so->sign);
	so->sign = NULL;

	if (scankey && scan->numberOfKeys > 0)
	{
		memmove(scan->keyData, scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));
	}

	PG_RETURN_VOID();
}

/*
 * End scan of bloom index.
 */
Datum
blendscan(PG_FUNCTION_ARGS)
{
	IndexScanDesc	scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	BloomScanOpaque so = (BloomScanOpaque) scan->opaque;

	if (so->sign)
		pfree(so->sign);
	so->sign = NULL;

	PG_RETURN_VOID();
}

/*
 * Insert all matching tuples into a bitmap.
 */
Datum
blgetbitmap(PG_FUNCTION_ARGS)
{
	/* We ignore the second argument as we're returning a hash bitmap */
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	Node		**bmNodeP = (Node **)PG_GETARG_POINTER(1);
	int64		ntids = 0;
	BlockNumber blkno = BLOOM_HEAD_BLKNO,
				npages;
	int			i;
	BufferAccessStrategy bas;
	TIDBitmap	*tbm;
	BloomScanOpaque so = (BloomScanOpaque) scan->opaque;


	/*
	 * GPDB specific code. Since GPDB also support StreamBitmap
	 * in bitmap index. So normally we need to create specific bitmap
	 * node in the amgetbitmap AM.
	 */
	Assert(bmNodeP);
	if (*bmNodeP == NULL)
	{
		/* XXX should we use less than work_mem for this? */
		tbm = tbm_create(work_mem * 1024L);
		*bmNodeP = (Node *) tbm;
	}
	else if (!IsA(*bmNodeP, TIDBitmap))
		elog(ERROR, "non btree bitmap");
	else
		tbm = (TIDBitmap *)*bmNodeP;

	if (so->sign == NULL)
	{
		/* New search: have to calculate search signature */
		ScanKey		skey = scan->keyData;

		so->sign = palloc0(sizeof(BloomSignatureWord) * so->state.opts.bloomLength);

		for (i = 0; i < scan->numberOfKeys; i++)
		{
			/*
			 * Assume bloom-indexable operators to be strict, so nothing could
			 * be found for NULL key.
			 */
			if (skey->sk_flags & SK_ISNULL)
			{
				pfree(so->sign);
				so->sign = NULL;
				return 0;
			}

			/* Add next value to the signature */
			signValue(&so->state, so->sign, skey->sk_argument,
					  skey->sk_attno - 1);

			skey++;
		}
	}

	/*
	 * We're going to read the whole index. This is why we use appropriate
	 * buffer access strategy.
	 */
	bas = GetAccessStrategy(BAS_BULKREAD);
	npages = RelationGetNumberOfBlocks(scan->indexRelation);

	for (blkno = BLOOM_HEAD_BLKNO; blkno < npages; blkno++)
	{
		Buffer		buffer;
		Page		page;

		buffer = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM,
									blkno, RBM_NORMAL, bas);

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);

		/* thats a check for snapshot too old feature
		 https://git.postgresql.org/gitweb/?p=postgresql.git;a=commit;h=848ef42bb8c7909c9d7baa38178d4a209906e7c1 */
#if 0
		TestForOldSnapshot(scan->xs_snapshot, scan->indexRelation, page);
#endif

		if (!PageIsNew(page) && !BloomPageIsDeleted(page))
		{
			OffsetNumber offset,
						maxOffset = BloomPageGetMaxOffset(page);

			for (offset = 1; offset <= maxOffset; offset++)
			{
				BloomTuple *itup = BloomPageGetTuple(&so->state, page, offset);
				bool		res = true;

				/* Check index signature with scan signature */
				for (i = 0; i < so->state.opts.bloomLength; i++)
				{
					if ((itup->sign[i] & so->sign[i]) != so->sign[i])
					{
						res = false;
						break;
					}
				}

				/* Add matching tuples to bitmap */
				if (res)
				{
					tbm_add_tuples(tbm, &itup->heapPtr, 1, true);
					ntids++;
				}
			}
		}

		UnlockReleaseBuffer(buffer);
		CHECK_FOR_INTERRUPTS();
	}
	FreeAccessStrategy(bas);

	PG_RETURN_INT64(ntids);
}
