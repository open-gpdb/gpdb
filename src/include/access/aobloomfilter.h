#ifndef AOBLOOMFILTER_H
#define AOBLOOMFILTER_H

#include "lib/bloomfilter.h"

#include "access/hash.h"
#include "lib/bloomfilter.h"

#define AOBLF_SIZE 128

#define BLOOM_F_MAGIC 0xD0D0CACA

typedef struct BloomFilterFixed {
    uint32      magic;
    /* K hash functions are used, seeded by caller's seed */
    int64       offset;    
    int			k_hash_funcs;
    uint64		seed;
    /* m is bitset size, in bits.  Must be a power of two <= 2^32.  */
    unsigned char bitset[AOBLF_SIZE];  
} BloomFilterFixed;

#define AO_BLF_SIZE (4 + 8 + AOBLF_SIZE)
#define AOBLF_N_FILTERS (32500 / AOBLF_SIZE)

typedef struct AOBFPageData {
    BloomFilterFixed filters[AOBLF_N_FILTERS];
} AOBFPageData;

typedef AOBFPageData *AOBFPage;

#define AOBLF_CALC_PAGE(x) (x / AOBLF_N_FILTERS)
#define AOBLF_CALC_OFFSET(x) (x % AOBLF_N_FILTERS)

extern bloom_filter *ao_bloom_filter_deserealize(BloomFilterFixed *bf);
extern BloomFilterFixed *ao_bloom_filter_serealize(bloom_filter *bf, int64 off);

extern void SaveBloomFilterForBlock(Relation aorel, bloom_filter *filter, int64 offset, int64 aoblknum);
extern bloom_filter *FetchBloomFilterForVarblock(Relation aorel, int64 varblocknum);

#endif /* AOBLOOMFILTER_H */