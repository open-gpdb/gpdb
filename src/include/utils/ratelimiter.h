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
extern void RateLimit(void *limiter);

#endif   /* RATELIMITER_H */
