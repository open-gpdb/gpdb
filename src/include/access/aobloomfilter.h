#ifndef AOBLOOMFILTER_H
#define AOBLOOMFILTER_H

#include "lib/bloomfilter.h"

#include "access/hash.h"
#include "lib/bloomfilter.h"

#define AOBLF_SIZE 128

typedef struct BloomFilterFixed {
    /* K hash functions are used, seeded by caller's seed */
    int			k_hash_funcs;
    uint64		seed;
    /* m is bitset size, in bits.  Must be a power of two <= 2^32.  */
    unsigned char bitset[AOBLF_SIZE];  
} BloomFilterFixed;

#define AO_BLF_SIZE (4 + 8 + AOBLF_SIZE)
#define AOBLF_N_FILTERS (8000 / AOBLF_SIZE)

typedef struct AOBFPageData {
    BloomFilterFixed filters[AOBLF_N_FILTERS];
} AOBFPageData;

typedef AOBFPageData *AOBFPage;

#define AOBLF_CALC_PAGE(x) (x / AOBLF_N_FILTERS)
#define AOBLF_CALC_OFFSET(x) (x % AOBLF_N_FILTERS)

extern bloom_filter* ao_bloom_filter_deserealize(BloomFilterFixed *bf);
extern BloomFilterFixed* ao_bloom_filter_serealize(bloom_filter *bf);


extern void SaveBloomFilterForBlock(Relation aorel, bloom_filter *filter, int aoblknum);

#endif /* AOBLOOMFILTER_H */