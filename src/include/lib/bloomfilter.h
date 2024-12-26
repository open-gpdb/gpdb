/*-------------------------------------------------------------------------
 *
 * bloomfilter.h
 *	  Space-efficient set membership testing
 *
 * Copyright (c) 2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/include/lib/bloomfilter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#define MAX_HASH_FUNCS		10

struct bloom_filter
{
	/* K hash functions are used, seeded by caller's seed */
	int			k_hash_funcs;
	uint64		seed;
	/* m is bitset size, in bits.  Must be a power of two <= 2^32.  */
	uint64		m;
	unsigned char bitset[FLEXIBLE_ARRAY_MEMBER];
};

typedef struct bloom_filter bloom_filter;

extern bloom_filter *bloom_create(int64 total_elems, int bloom_work_mem,
			 uint64 seed);
extern bloom_filter *bloom_create_nbytes(int64 nbytes, uint64 seed);

extern void bloom_free(bloom_filter *filter);
extern void bloom_add_element(bloom_filter *filter, unsigned char *elem,
				  size_t len);
extern bool bloom_lacks_element(bloom_filter *filter, unsigned char *elem,
					size_t len);
extern double bloom_prop_bits_set(bloom_filter *filter);

#endif							/* BLOOMFILTER_H */
