
#include "postgres.h"

#include "utils/rel.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"
#include "lib/bloomfilter.h"

#include "access/heapam_xlog.h"

#include "access/aobloomfilter.h"

void
SaveBloomFilterForBlock(Relation aorel, bloom_filter *filter, int64 offset, int64 varblocknum)
{
    Buffer		buffer;
    Page		page;
    BlockNumber blkno;
    OffsetNumber page_offset;
    BloomFilterFixed *fixed_filter;
    char * pointer;

    fixed_filter = ao_bloom_filter_serealize(filter, offset);
    fixed_filter->magic = BLOOM_F_MAGIC;

    blkno = AOBLF_CALC_PAGE(varblocknum);
    page_offset = AOBLF_CALC_OFFSET(varblocknum);

	buffer = ReadBufferExtended(aorel, FSM_FORKNUM, blkno, RBM_NORMAL, NULL);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buffer);

    pointer = PageGetContents(page) + page_offset * sizeof(BloomFilterFixed);

    memcpy(pointer, fixed_filter, sizeof(BloomFilterFixed));

    MarkBufferDirty(buffer);

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);
}

bloom_filter *
FetchBloomFilterForVarblock(Relation aorel, int64 varblocknum, int64 *next_offset)
{
    Buffer		buffer;
    Page		page;
    BlockNumber blkno;
    OffsetNumber page_offset;
    BloomFilterFixed *fixed_filter;
    char * pointer;

    blkno = AOBLF_CALC_PAGE(varblocknum);
    page_offset = AOBLF_CALC_OFFSET(varblocknum);

    fixed_filter = palloc0(sizeof(BloomFilterFixed));

	buffer = ReadBufferExtended(aorel, FSM_FORKNUM, blkno, RBM_NORMAL, NULL);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);


    pointer = PageGetContents(page) + page_offset * sizeof(BloomFilterFixed);

    memcpy(fixed_filter, pointer, sizeof(BloomFilterFixed));

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);

    if (fixed_filter->magic != BLOOM_F_MAGIC)
        return NULL;

    *next_offset = fixed_filter->offset;

    return ao_bloom_filter_deserealize(fixed_filter);
}

bloom_filter *
ao_bloom_filter_deserealize(BloomFilterFixed *bf)
{
    bloom_filter * blf;
    blf = bloom_create_nbytes(AOBLF_SIZE, bf->k_hash_funcs, bf->seed);

    blf->m = AOBLF_SIZE;
    blf->seed = bf->seed;
    memcpy(blf->bitset, bf->bitset, sizeof(char) * AOBLF_SIZE);

    return blf;
}

BloomFilterFixed *
ao_bloom_filter_serealize(bloom_filter *bf, int64 off)
{
    BloomFilterFixed *blff;

    blff = palloc0(sizeof(BloomFilterFixed));

    blff->offset = off;
    blff->k_hash_funcs = bf->k_hash_funcs;
    blff->seed = bf->seed;

    memcpy(blff->bitset, bf->bitset, sizeof(char) * AOBLF_SIZE);

    return blff;
}