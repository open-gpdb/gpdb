
#include "postgres.h"

#include "utils/rel.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"

#include "access/aobloomfilter.h"

void SaveBloomFilterForBlock(Relation aorel, bloom_filter *filter, int aoblknum)
{
    Buffer		buffer;
    Page		page;
    BlockNumber blkno;
    AOBFPage metapage;
    BloomFilterFixed *fixed_filter;
    char * pointer;

    fixed_filter = ao_bloom_filter_serealize(filter);

    blkno = AOBLF_CALC_PAGE(aoblknum);

	buffer = ReadBufferExtended(aorel, VISIBILITYMAP_FORKNUM, blkno, RBM_NORMAL, NULL);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buffer);

	metapage = PageGetContents(page);

    pointer = PageGetSpecialPointer(page) + AOBLF_CALC_OFFSET(aoblknum) * sizeof(BloomFilterFixed);

    memcpy(pointer, fixed_filter, sizeof(BloomFilterFixed));

	MarkBufferDirtyHint(buffer, false);

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
}

bloom_filter *
ao_bloom_filter_deserealize(BloomFilterFixed *bf)
{

}

BloomFilterFixed *
ao_bloom_filter_serealize(bloom_filter *bf)
{

}