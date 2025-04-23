
#include "postgres.h"

#include "access/htup_details.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "miscadmin.h"

#include "fmgr.h"
#include "access/heapam.h"
#include "utils/snapmgr.h"
#include "catalog/pg_namespace.h"
#include "utils/syscache.h"

#include "cdb/cdbaocsam.h"
#include "cdb/cdbappendonlyam.h"

PG_FUNCTION_INFO_V1(get_ao_headers_info);
PG_FUNCTION_INFO_V1(get_aocs_headers_info);

typedef struct AOHeadersInfoCxt {
    AppendOnlyScanDesc scan;
    TupleTableSlot *slot;
} AOHeadersInfoCxt;

#define NUM_GET_AO_HEADERS_INFO 9
#define NUM_GET_AOCS_HEADERS_INFO 8

Datum
get_ao_headers_info(PG_FUNCTION_ARGS)
{
    Relation r;
    AppendOnlyScanDesc scan;
    TupleTableSlot *slot;
    FuncCallContext *funcctx;
    MemoryContext oldcontext;
    HeapTuple tuple;
    Datum result;
    /* segment if loaded */
    Datum values[NUM_GET_AO_HEADERS_INFO];
    bool nulls[NUM_GET_AO_HEADERS_INFO];

    // int colno = PG_GETARG_INT32(1);	AOCSHeaderScanDesc hdesc;
    Oid	   oid = PG_GETARG_OID(0);


	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use raw page functions"))));
 
    r = relation_open(oid, AccessShareLock);

    if (!RelationIsAoRows(r))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 (errmsg("must be an Append-Optimized relation to use raw headerfunctions"))));

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
        * switch to memory context appropriate for multiple function calls
        */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);


        scan = appendonly_beginscan(r, GetActiveSnapshot(),
                                                    GetActiveSnapshot(), 0, NULL);

        slot = MakeSingleTupleTableSlot(RelationGetDescr(r));

        funcctx->tuple_desc =
            CreateTemplateTupleDesc(NUM_GET_AO_HEADERS_INFO, false);


        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)1, "first row number", INT8OID,
                        -1 /* typmod */, 0 /* attdim */);
        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)2, "large read position", INT8OID,
                        -1 /* typmod */, 0 /* attdim */);
        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)3, "buffer offset", INT4OID,
                        -1 /* typmod */, 0 /* attdim */);
        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)4, "block kind", TEXTOID,
                        -1 /* typmod */, 0 /* attdim */);
        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)5, "header kind", TEXTOID,
                        -1 /* typmod */, 0 /* attdim */);
        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)6, "current item count", INT4OID,
                        -1 /* typmod */, 0 /* attdim */);
        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)7, "isCompressed",
                        BOOLOID, -1 /* typmod */, 0 /* attdim */);
        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)8, "isLarge",
                        BOOLOID, -1 /* typmod */, 0 /* attdim */);
        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)9,
                        "dataLen", INT4OID, -1 /* typmod */,
                        0 /* attdim */);

        funcctx->tuple_desc = BlessTupleDesc(funcctx->tuple_desc);

        funcctx->user_fctx = palloc(sizeof(AOHeadersInfoCxt));

        ((AOHeadersInfoCxt*)funcctx->user_fctx)->scan = scan;
        ((AOHeadersInfoCxt*)funcctx->user_fctx)->slot = slot;

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    scan = ((AOHeadersInfoCxt*)funcctx->user_fctx)->scan;
    slot = ((AOHeadersInfoCxt*)funcctx->user_fctx)->slot;

    for (;;)
    {       
        if (scan->aos_need_new_segfile)
        {
            /*
            * Need to open a new segment file.
            */
            if (!SetNextFileSegForRead(scan))
            {
                break;
            }
        }
        
        if (!AppendOnlyExecutorReadBlock_GetBlockInfo(
                                                    &scan->storageRead,
                                                    &scan->executorReadBlock))
        {
            /* done reading the file */
            AppendOnlyStorageRead_CloseFile(&scan->storageRead);
            break;
        }

        elog(DEBUG5, "block stat: first row num: %ld, largeReadPosition %ld, bufferOffset: %d, current item count: %d, isCompressed: %d, isLarge: %d, dataLen: %d", 
            scan->executorReadBlock.blockFirstRowNum,
            scan->storageRead.bufferedRead.largeReadPosition,
            scan->storageRead.bufferedRead.bufferOffset,
            scan->executorReadBlock.currentItemCount,
            scan->executorReadBlock.isCompressed, scan->executorReadBlock.isLarge,
            scan->executorReadBlock.dataLen);

        values[0] = Int64GetDatum(scan->executorReadBlock.blockFirstRowNum);
        values[1] = Int64GetDatum(scan->storageRead.bufferedRead.largeReadPosition);
        values[2] = Int32GetDatum(scan->storageRead.bufferedRead.bufferOffset);

        switch (scan->executorReadBlock.executorBlockKind)
        {
            case AoExecutorBlockKind_VarBlock:
            values[3] = CStringGetTextDatum("varblock");
            break;
            case AoExecutorBlockKind_SingleRow:
            values[3] = CStringGetTextDatum("single row");
            break;
            default:
            values[3] = CStringGetTextDatum("unknown");
            break;
        }

        switch (scan->storageRead.current.headerKind)
        {
            case AoHeaderKind_SmallContent:
            values[4] = CStringGetTextDatum("small content");
            break;
            case AoHeaderKind_LargeContent:
            values[4] = CStringGetTextDatum("large content");
            break;
            case AoHeaderKind_NonBulkDenseContent:
            values[4] = CStringGetTextDatum("non bulk dense content");
            break;
            case AoHeaderKind_BulkDenseContent:
            values[4] = CStringGetTextDatum("bulk dense content");
            break;
            default:
            values[4] = CStringGetTextDatum("unknown");
            break;
        }

        values[5] = Int32GetDatum(scan->executorReadBlock.currentItemCount);
        values[6] = BoolGetDatum(scan->executorReadBlock.isCompressed);
        values[7] = BoolGetDatum(scan->executorReadBlock.isLarge);
        values[8] = Int32GetDatum(scan->executorReadBlock.dataLen);

        AppendOnlyExecutorReadBlock_GetContents(
                                                &scan->executorReadBlock);
        MemSet(nulls, 0, sizeof(nulls));

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);

        for (;;)
        {
            bool		found;

            found = AppendOnlyExecutorReadBlock_ScanNextTuple(&scan->executorReadBlock,
                                                            0,
                                                            NULL,
                                                            slot);

            if (found)
                continue;
            
            scan->needNextBuffer = true;
            break;
        }

        relation_close(r, AccessShareLock);
        SRF_RETURN_NEXT(funcctx, result);
    }


    ExecDropSingleTupleTableSlot(slot);
    appendonly_endscan(scan);

    relation_close(r, AccessShareLock);
    SRF_RETURN_DONE(funcctx);
}


Datum
get_aocs_headers_info(PG_FUNCTION_ARGS)
{
    Relation r;
    AOCSScanDesc scan;
    TupleTableSlot *slot;
    FuncCallContext *funcctx;
    MemoryContext oldcontext;
    HeapTuple tuple;
    Datum result;
    /* segment if loaded */
    Datum values[NUM_GET_AOCS_HEADERS_INFO];
    bool nulls[NUM_GET_AOCS_HEADERS_INFO];
    int nvp;
    bool *proj;
    int i;
	int64		rowNum = INT64CONST(-1);
    // int colno = PG_GETARG_INT32(1);	AOCSHeaderScanDesc hdesc;
    Oid	   oid = PG_GETARG_OID(0);



	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use raw page functions"))));
 
    r = relation_open(oid, AccessShareLock);

    if (!RelationIsAoCols(r))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 (errmsg("must be an AOCS relation to use raw headerfunctions"))));

    nvp = r->rd_att->natts;
    proj = palloc0(sizeof(bool) * nvp);

    for (i = 0; i < nvp; ++ i)
        proj[i] = 1;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
        * switch to memory context appropriate for multiple function calls
        */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);


        scan = aocs_beginscan(r, GetActiveSnapshot(),
                                                    GetActiveSnapshot(), NULL, proj);

        slot = MakeSingleTupleTableSlot(RelationGetDescr(r));

        funcctx->tuple_desc =
            CreateTemplateTupleDesc(NUM_GET_AOCS_HEADERS_INFO, false);

        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)1, "attribute number", INT4OID,
                        -1 /* typmod */, 0 /* attdim */);
        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)2, "large read position", INT8OID,
                        -1 /* typmod */, 0 /* attdim */);
        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)3, "buffer offset", INT4OID,
                        -1 /* typmod */, 0 /* attdim */);
        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)4, "header kind", TEXTOID,
                        -1 /* typmod */, 0 /* attdim */);                       
        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)5,
                        "first row", INT4OID, -1 /* typmod */,
                        0 /* attdim */);
        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)6, "isCompressed",
                        BOOLOID, -1 /* typmod */, 0 /* attdim */);
        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)7, "isLarge",
                        BOOLOID, -1 /* typmod */, 0 /* attdim */);
        TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)8,
                        "row count", INT4OID, -1 /* typmod */,
                        0 /* attdim */);

        funcctx->tuple_desc = BlessTupleDesc(funcctx->tuple_desc);

        funcctx->user_fctx = palloc(sizeof(AOHeadersInfoCxt));

        ((AOHeadersInfoCxt*)funcctx->user_fctx)->scan = scan;
        ((AOHeadersInfoCxt*)funcctx->user_fctx)->slot = slot;

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    scan = ((AOHeadersInfoCxt*)funcctx->user_fctx)->scan;
    slot = ((AOHeadersInfoCxt*)funcctx->user_fctx)->slot;

	while (1)
	{
        AOCSFileSegInfo *curseginfo;
        int			err = 0;

        Datum	   *d = slot_get_values(slot);
        bool	   *null = slot_get_isnull(slot);

ReadNext:
		/* If necessary, open next seg */
		if (scan->cur_seg < 0 || err < 0)
		{
			err = open_next_scan_seg(scan);
			if (err < 0)
			{
				/* No more seg, we are at the end */
				ExecClearTuple(slot);
				scan->cur_seg = -1;

                ExecDropSingleTupleTableSlot(slot);
                aocs_endscan(scan);

                relation_close(r, AccessShareLock);
                SRF_RETURN_DONE(funcctx);
			}
			scan->cur_seg_row = 0;
		}

		Assert(scan->cur_seg >= 0);
		curseginfo = scan->seginfo[scan->cur_seg];

		/* Read from cur_seg */
		for (i = 0; i < scan->num_proj_atts; i++)
		{
			int			attno = scan->proj_atts[i];

			err = datumstreamread_advance(scan->ds[attno]);
			Assert(err >= 0);
			if (err == 0)
			{
				err = datumstreamread_block(scan->ds[attno], scan->blockDirectory, attno);
				if (err < 0)
				{
					/*
					 * Ha, cannot read next block, we need to go to next seg
					 */
					close_cur_scan_seg(scan);
					goto ReadNext;
				}

                values[0] = Int32GetDatum(i);
                values[1] = Int64GetDatum(scan->ds[i]->ao_read.bufferedRead.largeReadPosition);
                values[2] = Int32GetDatum(scan->ds[i]->ao_read.bufferedRead.bufferOffset);


                switch (scan->ds[i]->ao_read.current.headerKind)
                {
                    case AoHeaderKind_SmallContent:
                    values[3] = CStringGetTextDatum("small content");
                    break;
                    case AoHeaderKind_LargeContent:
                    values[3] = CStringGetTextDatum("large content");
                    break;
                    case AoHeaderKind_NonBulkDenseContent:
                    values[3] = CStringGetTextDatum("non bulk dense content");
                    break;
                    case AoHeaderKind_BulkDenseContent:
                    values[3] = CStringGetTextDatum("bulk dense content");
                    break;
                    default:
                    values[3] = CStringGetTextDatum("unknown");
                    break;
                }


                values[4] = Int64GetDatum(scan->ds[i]->getBlockInfo.firstRow);
                values[5] = Int32GetDatum(scan->ds[i]->getBlockInfo.isCompressed);
                values[6] = Int32GetDatum(scan->ds[i]->getBlockInfo.isLarge);
                values[7] = Int32GetDatum(scan->ds[i]->getBlockInfo.rowCnt);

                MemSet(nulls, 0, sizeof(nulls));

                tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
                result = HeapTupleGetDatum(tuple);

                relation_close(r, AccessShareLock);
                SRF_RETURN_NEXT(funcctx, result);
			}


			/*
			 * Get the column's datum right here since the data structures
			 * should still be hot in CPU data cache memory.
			 */
			datumstreamread_get(scan->ds[attno], &d[attno], &null[attno]);

			if (rowNum == INT64CONST(-1) &&
				scan->ds[attno]->blockFirstRowNum != INT64CONST(-1))
			{
				Assert(scan->ds[attno]->blockFirstRowNum > 0);
				rowNum = scan->ds[attno]->blockFirstRowNum +
					datumstreamread_nth(scan->ds[attno]);
			}
		}

		scan->cur_seg_row++;
    }


    ExecDropSingleTupleTableSlot(slot);
    aocs_endscan(scan);

    relation_close(r, AccessShareLock);
    SRF_RETURN_DONE(funcctx);
}
