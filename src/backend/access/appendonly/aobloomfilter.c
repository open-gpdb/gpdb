
#include "postgres.h"

#include "utils/rel.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"
#include "lib/bloomfilter.h"

#include "access/aobloomfilter.h"

void
SaveBloomFilterForBlock(Relation aorel, bloom_filter *filter, int64 offset, int varblocknum)
{
    Buffer		buffer;
    Page		page;
    BlockNumber blkno;
    BloomFilterFixed *fixed_filter;
    char * pointer;

    fixed_filter = ao_bloom_filter_serealize(filter, offset);

    blkno = AOBLF_CALC_PAGE(varblocknum);

	buffer = ReadBufferExtended(aorel, FSM_FORKNUM, blkno, RBM_NORMAL, NULL);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buffer);

    pointer = PageGetSpecialPointer(page) + AOBLF_CALC_OFFSET(varblocknum) * sizeof(BloomFilterFixed);

    memcpy(pointer, fixed_filter, sizeof(BloomFilterFixed));

	MarkBufferDirtyHint(buffer, false);

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);
}

bloom_filter *
ao_bloom_filter_deserealize(BloomFilterFixed *bf)
{

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