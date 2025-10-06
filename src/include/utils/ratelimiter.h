#ifndef RATELIMITER_H
#define RATELIMITER_H

typedef struct RateLimiterDesc
{
	char	   *limiter_name;
	long		time_frame;
	int			num_elements;
}	RateLimiterDesc;

extern Size RateLimiterShmemSize(void);
extern void *RateLimiterShmemInit(RateLimiterDesc limiter_desc);
extern void RateLimiterRunOrWait(void *limiter);
extern void RateLimiterReconfigure(void *limiter, int limit, int window);

#endif   /* RATELIMITER_H */
