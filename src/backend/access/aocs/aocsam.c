/*--------------------------------------------------------------------------
 *
 * aocsam.c
 *	  Append only columnar access methods
 *
 * Portions Copyright (c) 2009-2010, Greenplum Inc.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/access/aocs/aocsam.c
 *
 *--------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/relpath.h"
#include "access/aocssegfiles.h"
#include "access/aomd.h"
#include "access/appendonlytid.h"
#include "access/appendonlywriter.h"
#include "access/heapam.h"
#include "access/hio.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/namespace.h"
#include "catalog/pg_appendonly_fn.h"
#include "catalog/pg_attribute_encoding.h"
#include "commands/vacuum.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbappendonlyblockdirectory.h"
#include "cdb/cdbappendonlystoragelayer.h"
#include "cdb/cdbappendonlystorageread.h"
#include "cdb/cdbappendonlystoragewrite.h"
#include "cdb/cdbvars.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/datumstream.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "catalog/pg_namespace.h"
#include "utils/sampling.h"
#include "utils/syscache.h"


bool
aocs_get_target_tuple(AOCSScanDesc aoscan, int64 targrow, TupleTableSlot *slot);

static AOCSScanDesc aocs_beginscan_internal(Relation relation,
						AOCSFileSegInfo **seginfo,
						int total_seg,
						Snapshot snapshot,
						Snapshot appendOnlyMetaDataSnapshot,
						TupleDesc relationTupleDesc, bool *proj);

/*
 * Open the segment file for a specified column associated with the datum
 * stream.
 */
static void
open_datumstreamread_segfile(
							 char *basepath, RelFileNode node,
							 AOCSFileSegInfo *segInfo,
							 DatumStreamRead *ds,
							 int colNo)
{
	int			segNo = segInfo->segno;
	char		fn[MAXPGPATH];
	int32		fileSegNo;

	AOCSVPInfoEntry *e = getAOCSVPEntry(segInfo, colNo);

	FormatAOSegmentFileName(basepath, segNo, colNo, &fileSegNo, fn);
	Assert(strlen(fn) + 1 <= MAXPGPATH);

	Assert(ds);
	datumstreamread_open_file(ds, fn, e->eof, e->eof_uncompressed, node,
							  fileSegNo, segInfo->formatversion);
}

/*
 * Open all segment files associted with the datum stream.
 *
 * Currently, there is one segment file for each column. This function
 * only opens files for those columns which are in the projection.
 *
 * If blockDirectory is not NULL, the first block info is written to
 * the block directory.
 */
static void
open_all_datumstreamread_segfiles(Relation rel,
								  AOCSFileSegInfo *segInfo,
								  DatumStreamRead **ds,
								  int *proj_atts,
								  int num_proj_atts,
								  AppendOnlyBlockDirectory *blockDirectory)
{
	char	   *basepath = relpathbackend(rel->rd_node, rel->rd_backend, MAIN_FORKNUM);
	int			i;

	Assert(proj_atts);

	for (i = 0; i < num_proj_atts; i++)
	{
		int			attno = proj_atts[i];

		open_datumstreamread_segfile(basepath, rel->rd_node, segInfo, ds[attno], attno);
		datumstreamread_block(ds[attno], blockDirectory, attno);
	}

	pfree(basepath);
}

/*
 * Initialise data streams for every column used in this query. For writes, this
 * means all columns.
 */
static void
open_ds_write(Relation rel, DatumStreamWrite **ds, TupleDesc relationTupleDesc,
			  bool checksum)
{
	int			nvp = relationTupleDesc->natts;
	StdRdOptions **opts = RelationGetAttributeOptions(rel);

	/* open datum streams.  It will open segment file underneath */
	for (int i = 0; i < nvp; ++i)
	{
		Form_pg_attribute attr = relationTupleDesc->attrs[i];
		char	   *ct;
		int32		clvl;
		int32		blksz;

		StringInfoData titleBuf;

		/* UNDONE: Need to track and dispose of this storage... */
		initStringInfo(&titleBuf);
		appendStringInfo(&titleBuf,
						 "Write of Append-Only Column-Oriented relation '%s', column #%d '%s'",
						 RelationGetRelationName(rel),
						 i + 1,
						 NameStr(attr->attname));

		/*
		 * We always record all the three column specific attributes for each
		 * column of a column oriented table.  Note: checksum is a table level
		 * attribute.
		 */
		if (opts[i] == NULL || opts[i]->blocksize == 0)
			elog(ERROR, "No relation attribute options for '%s', column #%d  in pg_attribute_encoding",
							RelationGetRelationName(rel),
							i + 1);
		ct = opts[i]->compresstype;
		clvl = opts[i]->compresslevel;
		blksz = opts[i]->blocksize;

		HeapTuple tp;
		char * nspname;

		tp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(rel->rd_rel->relnamespace));

		if (HeapTupleIsValid(tp))
		{
			Form_pg_namespace nsptup = (Form_pg_namespace) GETSTRUCT(tp);
			nspname = pstrdup(NameStr(nsptup->nspname));
			ReleaseSysCache(tp);
		} else {
			elog(ERROR, "yezzey: failed to get namescape name of relation %s", rel->rd_rel->relname.data);
		}


		ds[i] = create_datumstreamwrite(ct,
										clvl,
										checksum,
										 /* safeFSWriteSize */ 0,	/* UNDONE: Need to wire
																	 * down pg_appendonly
																	 * column? */
										blksz,
										attr,
										nspname,
										RelationGetRelationName(rel),
										RelationGetRelid(rel),
										/* title */ titleBuf.data,
										XLogIsNeeded() && RelationNeedsWAL(rel));


		pfree(nspname);
	}

	for (int i = 0; i < RelationGetNumberOfAttributes(rel); i++)
			pfree(opts[i]);
	pfree(opts);
}

/*
 * Initialise data streams for every column used in this query. For writes, this
 * means all columns.
 */
static void
open_ds_read(Relation rel, DatumStreamRead **ds, TupleDesc relationTupleDesc,
			 int *proj_atts, int num_proj_atts, bool checksum)
{
	int			nvp = relationTupleDesc->natts;
	StdRdOptions **opts = RelationGetAttributeOptions(rel);
	int			i;

	/* Clear all the entries to NULL first. */
	for (i = 0; i < nvp; ++i)
		ds[i] = NULL;

	/* And then initialize the data streams for those columns we need */
	for (i = 0; i < num_proj_atts; i++)
	{
		int			attno = proj_atts[i];
		Form_pg_attribute attr = relationTupleDesc->attrs[attno];
		char	   *ct;
		int32		clvl;
		int32		blksz;
		StringInfoData titleBuf;

		/*
		 * We always record all the three column specific attributes for each
		 * column of a column oriented table.  Note: checksum is a table level
		 * attribute.
		 */
		if (opts[attno] == NULL || opts[attno]->blocksize == 0)
			elog(ERROR, "No relation attribute options for '%s', column #%d  in pg_attribute_encoding",
							RelationGetRelationName(rel),
							attno + 1);

		ct = opts[attno]->compresstype;
		clvl = opts[attno]->compresslevel;
		blksz = opts[attno]->blocksize;

		/* UNDONE: Need to track and dispose of this storage... */
		initStringInfo(&titleBuf);
		appendStringInfo(&titleBuf, "Scan of Append-Only Column-Oriented relation '%s', column #%d '%s'",
						 RelationGetRelationName(rel),
						 attno + 1,
						 NameStr(attr->attname));
		HeapTuple tp;
		char * nspname;

		tp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(rel->rd_rel->relnamespace));

		if (HeapTupleIsValid(tp))
		{
			Form_pg_namespace nsptup = (Form_pg_namespace) GETSTRUCT(tp);
			nspname = pstrdup(NameStr(nsptup->nspname));
			ReleaseSysCache(tp);
		} else {
			elog(ERROR, "yezzey: failed to get namescape name of relation %s", rel->rd_rel->relname.data);
		}

		ds[attno] = create_datumstreamread(ct,
										   clvl,
										   checksum,
										    /* safeFSWriteSize */ false,	/* UNDONE:Need to wire
																			 * down pg_appendonly
																			 * column */
										   blksz,
										   attr,
										   nspname,
										   RelationGetRelationName(rel),
										   rel->rd_id,
										    /* title */ titleBuf.data);

		pfree(nspname);
	}

	for (i = 0; i < RelationGetNumberOfAttributes(rel); i++)
			pfree(opts[i]);
	pfree(opts);
}

static void
close_ds_read(DatumStreamRead **ds, int nvp)
{
	int			i;

	for (i = 0; i < nvp; ++i)
	{
		if (ds[i])
		{
			destroy_datumstreamread(ds[i]);
			ds[i] = NULL;
		}
	}
}

static void
close_ds_write(DatumStreamWrite **ds, int nvp)
{
	int			i;

	for (i = 0; i < nvp; ++i)
	{
		if (ds[i])
		{
			destroy_datumstreamwrite(ds[i]);
			ds[i] = NULL;
		}
	}
}


static void
aocs_initscan(AOCSScanDesc scan)
{
	scan->cur_seg = -1;

	ItemPointerSet(&scan->cdb_fake_ctid, 0, 0);
	scan->cur_seg_row = 0;

	open_ds_read(scan->aos_rel, scan->ds, scan->relationTupleDesc,
				 scan->proj_atts, scan->num_proj_atts,
				 scan->aos_rel->rd_appendonly->checksum);

	pgstat_count_heap_scan(scan->aos_rel);
}

int
open_next_scan_seg(AOCSScanDesc scan)
{
	int			nvp = scan->relationTupleDesc->natts;

	while (++scan->cur_seg < scan->total_seg)
	{
		AOCSFileSegInfo *curSegInfo = scan->seginfo[scan->cur_seg];

		if (curSegInfo->total_tupcount > 0)
		{
			bool		emptySeg = false;

			/*
			 * If the segment is entirely empty, nothing to do.
			 *
			 * We assume the corresponding segments for every column to be in
			 * the same state. So somewhat arbitrarily, we check the state of
			 * the first column we'll be accessing.
			 */
			if (scan->num_proj_atts > 0)
			{
				AOCSVPInfoEntry *e = getAOCSVPEntry(curSegInfo, scan->proj_atts[0]);

				if (e->eof == 0 || curSegInfo->state == AOSEG_STATE_AWAITING_DROP)
					emptySeg = true;
			}

			if (!emptySeg)
			{

				/*
				 * If the scan also builds the block directory, initialize it
				 * here.
				 */
				if (scan->blockDirectory)
				{
					/*
					 * if building the block directory, we need to make sure
					 * the sequence starts higher than our highest tuple's
					 * rownum.  In the case of upgraded blocks, the highest
					 * tuple will have tupCount as its row num for non-upgrade
					 * cases, which use the sequence, it will be enough to
					 * start off the end of the sequence; note that this is
					 * not ideal -- if we are at least curSegInfo->tupcount +
					 * 1 then we don't even need to update the sequence value
					 */
					int64		firstSequence;

					firstSequence =
						GetFastSequences(scan->aos_rel->rd_appendonly->segrelid,
										 curSegInfo->segno,
										 curSegInfo->total_tupcount + 1,
										 NUM_FAST_SEQUENCES);

					AppendOnlyBlockDirectory_Init_forInsert(scan->blockDirectory,
															scan->appendOnlyMetaDataSnapshot,
															(FileSegInfo *) curSegInfo,
															0 /* lastSequence */ ,
															scan->aos_rel,
															curSegInfo->segno,
															nvp,
															true);

					InsertFastSequenceEntry(scan->aos_rel->rd_appendonly->segrelid,
											curSegInfo->segno,
											firstSequence);
				}

				open_all_datumstreamread_segfiles(scan->aos_rel,
												  curSegInfo,
												  scan->ds,
												  scan->proj_atts,
												  scan->num_proj_atts,
												  scan->blockDirectory);

				return scan->cur_seg;
			}
		}
	}

	return -1;
}

void
close_cur_scan_seg(AOCSScanDesc scan)
{
	int			nvp = scan->relationTupleDesc->natts;
	int			i;

	if (scan->cur_seg < 0)
		return;

	for (i = 0; i < nvp; ++i)
	{
		if (scan->ds[i])
			datumstreamread_close_file(scan->ds[i]);
	}

	if (scan->blockDirectory)
		AppendOnlyBlockDirectory_End_forInsert(scan->blockDirectory);
}

/*
 * aocs_beginrangescan
 *
 * begins range-limited relation scan
 */
AOCSScanDesc
aocs_beginrangescan(Relation relation,
					Snapshot snapshot,
					Snapshot appendOnlyMetaDataSnapshot,
					int *segfile_no_arr, int segfile_count,
					TupleDesc relationTupleDesc, bool *proj)
{
	AOCSFileSegInfo **seginfo;
	int			i;

	RelationIncrementReferenceCount(relation);

	seginfo = palloc0(sizeof(AOCSFileSegInfo *) * segfile_count);

	for (i = 0; i < segfile_count; i++)
	{
		seginfo[	i] = GetAOCSFileSegInfo(relation, appendOnlyMetaDataSnapshot,
											segfile_no_arr[i]);
	}
	return aocs_beginscan_internal(relation,
								   seginfo,
								   segfile_count,
								   snapshot,
								   appendOnlyMetaDataSnapshot,
								   relationTupleDesc,
								   proj);
}

AOCSScanDesc
aocs_beginscan(Relation relation,
			   Snapshot snapshot,
			   Snapshot appendOnlyMetaDataSnapshot,
			   TupleDesc relationTupleDesc, bool *proj)
{
	AOCSFileSegInfo **seginfo;
	int			total_seg;

	RelationIncrementReferenceCount(relation);

	seginfo = GetAllAOCSFileSegInfo(relation, appendOnlyMetaDataSnapshot, &total_seg);

	return aocs_beginscan_internal(relation,
								   seginfo,
								   total_seg,
								   snapshot,
								   appendOnlyMetaDataSnapshot,
								   relationTupleDesc,
								   proj);
}

/*
 * begin the scan over the given relation.
 *
 * 'relationTupleDesc' if NULL, then this function will simply use
 * relation->rd_att.  This is the typical use-case. Passing in a
 * separate tuple descriptor is only needed for cases for the caller has
 * changed relation->rd_att without updating the underlying relation files
 * yet (that is, the caller is doing an alter and relation->rd_att will be
 * the relation's new form but relationTupleDesc is the old form)
 */
static AOCSScanDesc
aocs_beginscan_internal(Relation relation,
						AOCSFileSegInfo **seginfo,
						int total_seg,
						Snapshot snapshot,
						Snapshot appendOnlyMetaDataSnapshot,
						TupleDesc relationTupleDesc, bool *proj)
{
	AOCSScanDesc scan;
	int			nvp;
	int			i;

	if (!relationTupleDesc)
		relationTupleDesc = relation->rd_att;

	nvp = relationTupleDesc->natts;

	scan = (AOCSScanDesc) palloc0(sizeof(AOCSScanDescData));
	scan->aos_rel = relation;
	scan->appendOnlyMetaDataSnapshot = appendOnlyMetaDataSnapshot;
	scan->snapshot = snapshot;

	scan->compLevel = relation->rd_appendonly->compresslevel;
	scan->compType = NameStr(relation->rd_appendonly->compresstype);
	scan->blocksz = relation->rd_appendonly->blocksize;

	scan->seginfo = seginfo;

	scan->total_seg = total_seg;
	scan->relationTupleDesc = relationTupleDesc;

	/*
	 * We get an array of booleans to indicate which columns are needed. But
	 * if you have a very wide table, and you only select a few columns from
	 * it, just scanning the boolean array to figure out which columns are
	 * needed can incur a noticeable overhead in aocs_getnext. So convert it
	 * into an array of the attribute numbers of the required columns.
	 */
	Assert(proj);
	scan->proj_atts = palloc(scan->relationTupleDesc->natts * sizeof(int));

	scan->num_proj_atts = 0;
	for (i = 0; i < scan->relationTupleDesc->natts; i++)
	{
		if (proj[i])
			scan->proj_atts[scan->num_proj_atts++] = i;
	}

	scan->ds = (DatumStreamRead **) palloc0(sizeof(DatumStreamRead *) * nvp);

	aocs_initscan(scan);

	scan->blockDirectory = NULL;

	AppendOnlyVisimap_Init(&scan->visibilityMap,
						   relation->rd_appendonly->visimaprelid,
						   relation->rd_appendonly->visimapidxid,
						   AccessShareLock,
						   appendOnlyMetaDataSnapshot);

	return scan;
}

/* ----------------
 *		aocs_afterscan	- perform after scan actions
 *
 * Release some structures, which is safe to free after initial scan, but
 * before rescan.
 * ----------------
 */
void
aocs_afterscan(AOCSScanDesc scan)
{
	int			nvp = scan->relationTupleDesc->natts;
	int			i;

	if (scan->cur_seg >= 0)
	{
		for (i = 0; i < nvp; ++i)
		{
			if (scan->ds[i])
				datumstreamread_close_file(scan->ds[i]);
		}
	}

	close_ds_read(scan->ds, scan->relationTupleDesc->natts);
}

void
aocs_rescan(AOCSScanDesc scan)
{
	aocs_afterscan(scan);
	aocs_initscan(scan);
}

void
aocs_endscan(AOCSScanDesc scan)
{
	int			i;

	RelationDecrementReferenceCount(scan->aos_rel);

	close_cur_scan_seg(scan);
	close_ds_read(scan->ds, scan->relationTupleDesc->natts);

	pfree(scan->proj_atts);
	scan->proj_atts = NULL;
	pfree(scan->ds);
	scan->ds = NULL;

	for (i = 0; i < scan->total_seg; ++i)
	{
		if (scan->seginfo[i])
		{
			pfree(scan->seginfo[i]);
			scan->seginfo[i] = NULL;
		}
	}
	if (scan->seginfo)
	{
		pfree(scan->seginfo);
		scan->seginfo = NULL;
	}

	AppendOnlyVisimap_Finish(&scan->visibilityMap, AccessShareLock);

	pfree(scan);
}

/*
 * Upgrades a Datum value from a previous version of the AOCS page format. The
 * DatumStreamRead that is passed must correspond to the column being upgraded.
 */
static void upgrade_datum_impl(DatumStreamRead *ds, int attno, Datum values[],
							   bool isnull[], int formatversion)
{
	bool 	convert_numeric = false;

	if (PG82NumericConversionNeeded(formatversion))
	{
		/*
		 * On the first call for this DatumStream, figure out if this column is
		 * a numeric, or a domain over numerics.
		 *
		 * TODO: consolidate this code with upgrade_tuple() in appendonlyam.c.
		 */
		if (!OidIsValid(ds->baseTypeOid))
		{
			ds->baseTypeOid = getBaseType(ds->typeInfo.typid);
		}

		/* If this Datum is a numeric, we need to convert it. */
		convert_numeric = (ds->baseTypeOid == NUMERICOID) && !isnull[attno];
	}

	if (convert_numeric)
	{
		/*
		 * Before PostgreSQL 8.3, the n_weight and n_sign_dscale fields were the
		 * other way 'round. Swap them.
		 */
		Datum 		datum;
		char	   *numericdata;
		char	   *upgradedata;
		size_t		datalen;
		uint16		tmp;

		/*
		 * We need to make a copy of this data so that any other tuples pointing
		 * to it won't be affected. Store it in the upgrade space for this
		 * DatumStream.
		 */
		datum = values[attno];
		datalen = VARSIZE_ANY(DatumGetPointer(datum));

		upgradedata = datumstreamread_get_upgrade_space(ds, datalen);
		memcpy(upgradedata, DatumGetPointer(datum), datalen);

		/* Swap the fields. */
		numericdata = VARDATA_ANY(upgradedata);

		memcpy(&tmp, &numericdata[0], 2);
		memcpy(&numericdata[0], &numericdata[2], 2);
		memcpy(&numericdata[2], &tmp, 2);

		/* Re-point the Datum to the upgraded numeric. */
		values[attno] = PointerGetDatum(upgradedata);
	}
}

static void upgrade_datum_scan(AOCSScanDesc scan, int attno, Datum values[],
							   bool isnull[], int formatversion)
{
	upgrade_datum_impl(scan->ds[attno], attno, values, isnull, formatversion);
}

static void upgrade_datum_fetch(AOCSFetchDesc fetch, int attno, Datum values[],
								bool isnull[], int formatversion)
{
	upgrade_datum_impl(fetch->datumStreamFetchDesc[attno]->datumStream, attno,
					   values, isnull, formatversion);
}

static int
aocs_locate_target_segment(AOCSScanDesc scan, int64 targrow)
{
	int64 rowcount;

	for (int i = scan->cur_seg; i < scan->total_seg; i++)
	{
		if (i < 0)
			continue;

		rowcount = scan->seginfo[i]->total_tupcount;
		if (rowcount <= 0)
			continue;

		if (scan->segfirstrow + rowcount - 1 >= targrow)
		{
			/* found the target segment */
			return i;
		}

		/* continue next segment */
		scan->segfirstrow += rowcount;
		scan->segrowsprocessed = 0;
	}

	/* row is beyond the total number of rows in the relation */
	return -1;
}

/*
 * block directory based get_target_tuple()
 */
static bool
aocs_blkdirscan_get_target_tuple(AOCSScanDesc scan, int64 targrow, TupleTableSlot *slot)
{
	int segno, segidx;
	int64 rownum = -1;
	int64 rowsprocessed;
	AOTupleId aotid;
	int ncols = scan->aos_rel->rd_att->natts;
	
	AppendOnlyBlockDirectory *blkdir = &scan->aocsfetch->blockDirectory;
	Assert(scan->blkdirscan != NULL);

	/* locate the target segment */
	segidx = aocs_locate_target_segment(scan, targrow);

	if (segidx < 0)
		return false;

	/* next starting position in locating segfile */
	scan->cur_seg = segidx;
	segno = scan->seginfo[segidx]->segno;
	Assert(segno > InvalidFileSegNumber && segno <= AOTupleId_MaxSegmentFileNum);

	/*
	 * Note: It is safe to assume that the scan's segfile array and the
	 * blockdir's segfile array are identical. Otherwise, we should stop
	 * processing and throw an exception to make the error visible.
	 */
	if (blkdir->segmentFileInfo[segidx]->segno != segno)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("segfile array contents in both scan descriptor "
				 		"and block directory are not identical on "
						"append-optimized relation '%s'",
						RelationGetRelationName(blkdir->aoRel))));
	}

	/*
	 * Unlike ao_row, we set currentSegmentFileNum for ao_column here
	 * just for passing the assertion in extract_minipage() called by
	 * AOBlkDirScan_GetRowNum().
	 * Since we don't invoke AppendOnlyBlockDirectory_GetCachedEntry()
	 * for ao_column, it shoule be restored back to the original value
	 * for AppendOnlyBlockDirectory_GetEntry() working properly.
	 */
	int currentSegmentFileNum = blkdir->currentSegmentFileNum;
	blkdir->currentSegmentFileNum = blkdir->segmentFileInfo[segidx]->segno;

	/* locate the target row by seqscan block directory */

	for (int col = 0; col < ncols; col++)
	{
		/*
		 * "segfirstrow" should be always pointing to the first row of
		 * a new segfile, only locate_target_segment could update
		 * its value.
		 * 
		 * "segrowsprocessed" is used for tracking the position of
		 * processed rows in the current segfile.
		 */

		rowsprocessed = scan->segfirstrow + scan->segrowsprocessed;

		if (scan->aos_rel->rd_att->attrs[col]->attisdropped)
			continue;

		rownum = AOBlkDirScan_GetRowNum(scan->blkdirscan,
										segno,
										col,
										targrow,
										&rowsprocessed);

		elog(DEBUG2, "AOBlkDirScan_GetRowNum(segno: %d, col: %d, targrow: %ld): "
			 "[segfirstrow: %ld, segrowsprocessed: %ld, rownum: %ld, cached_mpentry_num: %d]",
			 segno, col, targrow, scan->segfirstrow, scan->segrowsprocessed, rownum,
			 blkdir->cached_mpentry_num);

		if (rownum < 0)
			continue;

		scan->segrowsprocessed = rowsprocessed - scan->segfirstrow;

		/*
		 * Found a column represented in the block directory.
		 * Here we just look for the 1st such column, no need
		 * to read other columns within the same row.
		 */
		break;
	}

	/* restore to the original value as above mentioned */
	blkdir->currentSegmentFileNum = currentSegmentFileNum;

	if (rownum < 0)
		return false;

	/* form the target tuple TID */
	AOTupleIdInit(&aotid, segno, rownum);
	ExecClearTuple(slot);

	/* fetch the target tuple */
	if(!aocs_fetch(scan->aocsfetch, &aotid, slot))
		return false;

	/* OK to return this tuple */


	ExecStoreVirtualTuple(slot);
	pgstat_count_heap_fetch(scan->aos_rel);
	return true;
}


/*
 * returns the segfile number in which `targrow` locates  
 */
static int
aocs_getsegment(AOCSScanDesc scan, int64 targrow)
{
	int segno, segidx;

	/* locate the target segment */
	segidx = aocs_locate_target_segment(scan, targrow);

	if (segidx < 0)
	{
		/* done reading all segments */
		close_cur_scan_seg(scan);
		scan->cur_seg = -1;
		return -1;
	}

	segno = scan->seginfo[segidx]->segno;
	Assert(segno > InvalidFileSegNumber && segno <= AOTupleId_MaxSegmentFileNum);

	if (segidx > scan->cur_seg)
	{
		close_cur_scan_seg(scan);
		/* adjust cur_seg to fit for open_next_scan_seg() */
		scan->cur_seg = segidx - 1;

		if (open_next_scan_seg(scan) >= 0)
		{
			/* new segment, make sure segrowsprocessed was reset */
			Assert(scan->segrowsprocessed == 0);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Unexpected behavior, failed to open segno %d during scanning AOCO table %s",
							segno, RelationGetRelationName(scan->aos_rel))));
		}
	}

	return segno;
}

static inline int
aocs_block_remaining_rows(DatumStreamRead *ds)
{
	return (ds->blockRowCount - ds->blockRowsProcessed);
}

/*
 * fetches a single column value corresponding to `endrow` (equals to `targrow`)
 */
static bool
aocs_gettuple_column(AOCSScanDesc scan, AttrNumber attno, int64 startrow, int64 endrow, bool chkvisimap, TupleTableSlot *slot)
{
	bool isSnapshotAny = (scan->snapshot == SnapshotAny);
	DatumStreamRead *ds = scan->ds[attno];
	int segno = scan->seginfo[scan->cur_seg]->segno;
	AOTupleId aotid;
	bool ret = true;
	int64 rowstoprocess, nrows, rownum;
	Datum *values;
	bool *nulls;

	if (ds->blockFirstRowNum <= 0)
		elog(ERROR, "AOCO varblock->blockFirstRowNum should be greater than zero.");


	Assert(segno > InvalidFileSegNumber && segno <= AOTupleId_MaxSegmentFileNum);
	Assert(startrow <= endrow);

	rowstoprocess = endrow - startrow + 1;
	nrows = ds->blockRowsProcessed + rowstoprocess;
	rownum = ds->blockFirstRowNum + nrows - 1;

	/* form the target tuple TID */
	AOTupleIdInit(&aotid, segno, rownum);

	if (chkvisimap && !isSnapshotAny && !AppendOnlyVisimap_IsVisible(&scan->visibilityMap, &aotid))
	{
		if (slot != NULL)
			ExecClearTuple(slot);
		
		ret = false;

		/* must update tracking vars before return */
		goto out;
	}

	/* rowNumInBlock = rowNum - blockFirstRowNum */
	datumstreamread_find(ds, rownum - ds->blockFirstRowNum);

	values = slot_get_values(slot);
	nulls = slot_get_isnull(slot);
	datumstreamread_get(ds, &(values[attno]), &(nulls[attno]));

out:
	/* update rows processed */
	ds->blockRowsProcessed += rowstoprocess;
	return ret;
}

/*
 * fetches all columns of the target tuple corresponding to `targrow`
 */
static bool
aocs_gettuple(AOCSScanDesc scan, int64 targrow, TupleTableSlot *slot)
{
	bool ret = true;
	int64 rowcount = -1;
	int64 rowstoprocess;
	bool chkvisimap = true;

	Assert(scan->cur_seg >= 0);
	Assert(slot != NULL);

	ExecClearTuple(slot);
	rowstoprocess = targrow - scan->segfirstrow + 1;

	/* read from scan->cur_seg */
	for (AttrNumber i = 0; i < scan->num_proj_atts; i++)
	{
		AttrNumber attno = scan->proj_atts[i];
		DatumStreamRead *ds = scan->ds[attno];
		int64 startrow = scan->segfirstrow + scan->segrowsprocessed;

		if (ds->blockRowCount <= 0)
			; /* haven't read block */
		else
		{
			/* block was read */
			rowcount = aocs_block_remaining_rows(ds);
			Assert(rowcount >= 0);

			if (startrow + rowcount - 1 >= targrow)
			{
				if (!aocs_gettuple_column(scan, attno, startrow, targrow, chkvisimap, slot))
				{
					ret = false;
					/* must update tracking vars before return */

					goto out;
				}

				chkvisimap = false;

				/* haven't finished scanning on current block */
				continue;
			}
			else
				startrow += rowcount; /* skip scanning remaining rows */
		}

		/*
		 * Keep reading block headers until we find the block containing
		 * the target row.
		 */

		while (true)
		{
			elog(DEBUG2, "aocs_gettuple(): [targrow: %ld, currow: %ld, diff: %ld, "
				 "startrow: %ld, rowcount: %ld, segfirstrow: %ld, segrowsprocessed: %ld, nth: %d, "
				 "blockRowCount: %d, blockRowsProcessed: %d]", targrow, startrow + rowcount - 1,
				 startrow + rowcount - 1 - targrow, startrow, rowcount, scan->segfirstrow,
				 scan->segrowsprocessed, datumstreamread_nth(ds), ds->blockRowCount,
				 ds->blockRowsProcessed);


			if (datumstreamread_block_info(ds))
			{
				rowcount = ds->blockRowCount;
				Assert(rowcount > 0);

				/* new block, reset blockRowsProcessed */
				ds->blockRowsProcessed = 0;

				if (startrow + rowcount - 1 >= targrow)
				{
					/* read a new buffer to consume */
					datumstreamread_block_content(ds);

					if (!aocs_gettuple_column(scan, attno, startrow, targrow, chkvisimap, slot))
					{
						ret = false;

						/* must update tracking vars before return */
						goto out;
					}

					chkvisimap = false;
					/* done this column */
					break;
				}

				startrow += rowcount;

				AppendOnlyStorageRead_SkipCurrentBlock(&ds->ao_read);

				/* continue next block */
			}
			else
				pg_unreachable(); /* unreachable code */
		}
	}

out:
	/* update rows processed */
	scan->segrowsprocessed = rowstoprocess;

	if (ret)
	{
		ExecStoreVirtualTuple(slot);
		pgstat_count_heap_getnext(scan->aos_rel);
	}

	return ret;
}


/*
 * Given a specific target row number 'targrow' (in the space of all row numbers
 * physically present in the table, i.e. across all segfiles), scan and return
 * the corresponding tuple in 'slot'.
 *
 * If the tuple is visible, return true. Otherwise, return false.
 */

bool
aocs_get_target_tuple(AOCSScanDesc aoscan, int64 targrow, TupleTableSlot *slot)
{
	if (aoscan->blkdirscan != NULL)
		return aocs_blkdirscan_get_target_tuple(aoscan, targrow, slot);


	if (aocs_getsegment(aoscan, targrow) < 0)
	{
		/* all done */

		ExecClearTuple(slot);
		return false;
	}

	/*
	 * Unlike AO_ROW, AO_COLUMN may have different varblocks
	 * for different columns, so we get per-column tuple directly
	 * on the way of walking per-column varblock.
	 */

	return aocs_gettuple(aoscan, targrow, slot);
}


bool
aocs_getnext(AOCSScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	int			ncol;
	Datum	   *d = slot_get_values(slot);
	bool	   *null = slot_get_isnull(slot);
	AOTupleId	aoTupleId;
	int64		rowNum = INT64CONST(-1);
	int			err = 0;
	int			i;
	bool		isSnapshotAny = (scan->snapshot == SnapshotAny);

	Assert(ScanDirectionIsForward(direction));

	ncol = slot->tts_tupleDescriptor->natts;
	Assert(ncol <= scan->relationTupleDesc->natts);

	while (1)
	{
		AOCSFileSegInfo *curseginfo;

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
				return false;
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

				err = datumstreamread_advance(scan->ds[attno]);
				Assert(err > 0);
			}

			/*
			 * Get the column's datum right here since the data structures
			 * should still be hot in CPU data cache memory.
			 */
			datumstreamread_get(scan->ds[attno], &d[attno], &null[attno]);

			/*
			 * Perform any required upgrades on the Datum we just fetched.
			 */
			if (curseginfo->formatversion < AORelationVersion_GetLatest())
			{
				upgrade_datum_scan(scan, attno, d, null,
								   curseginfo->formatversion);
			}

			if (rowNum == INT64CONST(-1) &&
				scan->ds[attno]->blockFirstRowNum != INT64CONST(-1))
			{
				Assert(scan->ds[attno]->blockFirstRowNum > 0);
				rowNum = scan->ds[attno]->blockFirstRowNum +
					datumstreamread_nth(scan->ds[attno]);
			}
		}

		scan->cur_seg_row++;
		if (rowNum == INT64CONST(-1))
		{
			AOTupleIdInit(&aoTupleId, curseginfo->segno, scan->cur_seg_row);
		}
		else
		{
			AOTupleIdInit(&aoTupleId, curseginfo->segno, rowNum);
		}

		if (!isSnapshotAny && !AppendOnlyVisimap_IsVisible(&scan->visibilityMap, &aoTupleId))
		{
			rowNum = INT64CONST(-1);
			goto ReadNext;
		}
		scan->cdb_fake_ctid = *((ItemPointer) &aoTupleId);

		TupSetVirtualTupleNValid(slot, ncol);
		slot_set_ctid(slot, &(scan->cdb_fake_ctid));
		return true;
	}

	Assert(!"Never here");
	return false;
}


/* Open next file segment for write.  See SetCurrentFileSegForWrite */
/* XXX Right now, we put each column to different files */
static void
OpenAOCSDatumStreams(AOCSInsertDesc desc)
{
	RelFileNodeBackend rnode;
	char	   *basepath;
	char		fn[MAXPGPATH];
	int32		fileSegNo;

	AOCSFileSegInfo *seginfo;

	TupleDesc	tupdesc = RelationGetDescr(desc->aoi_rel);
	int			nvp = tupdesc->natts;
	int			i;

	desc->ds = (DatumStreamWrite **) palloc0(sizeof(DatumStreamWrite *) * nvp);

	/*
	 * In order to append to this file segment entry we must first acquire the
	 * relation Append-Only segment file (transaction-scope) lock (tag
	 * LOCKTAG_RELATION_APPENDONLY_SEGMENT_FILE) in order to guarantee
	 * stability of the pg_aoseg information on this segment file and
	 * exclusive right to append data to the segment file.
	 *
	 * NOTE: This is a transaction scope lock that must be held until commit /
	 * abort.
	 */
	LockRelationAppendOnlySegmentFile(&desc->aoi_rel->rd_node,
									  desc->cur_segno,
									  AccessExclusiveLock,
									   /* dontWait */ false);

	open_ds_write(desc->aoi_rel, desc->ds, tupdesc,
				  desc->aoi_rel->rd_appendonly->checksum);

	/* Now open seg info file and get eof mark. */
	seginfo = GetAOCSFileSegInfo(desc->aoi_rel,
								 desc->appendOnlyMetaDataSnapshot,
								 desc->cur_segno);

	if (seginfo == NULL)
	{
		InsertInitialAOCSFileSegInfo(desc->aoi_rel, desc->cur_segno, nvp);
		seginfo = NewAOCSFileSegInfo(desc->cur_segno, nvp);
	}

	desc->fsInfo = seginfo;

	/* Never insert into a segment that is awaiting a drop */
	elogif(desc->fsInfo->state == AOSEG_STATE_AWAITING_DROP, ERROR,
		   "cannot insert into segno (%d) for AO relid %d that is in state AOSEG_STATE_AWAITING_DROP",
		   desc->cur_segno, RelationGetRelid(desc->aoi_rel));

	desc->rowCount = seginfo->total_tupcount;

	rnode.node = desc->aoi_rel->rd_node;
	rnode.backend = desc->aoi_rel->rd_backend;
	basepath = relpath(rnode, MAIN_FORKNUM);

	for (i = 0; i < nvp; ++i)
	{
		AOCSVPInfoEntry *e = getAOCSVPEntry(seginfo, i);

		FormatAOSegmentFileName(basepath, seginfo->segno, i, &fileSegNo, fn);
		Assert(strlen(fn) + 1 <= MAXPGPATH);

		datumstreamwrite_open_file(desc->ds[i], fn, e->eof, e->eof_uncompressed, seginfo->modcount,
								   &rnode,
								   fileSegNo, seginfo->formatversion);
	}

	pfree(basepath);
}

static inline void
SetBlockFirstRowNums(DatumStreamWrite **datumStreams,
					 int numDatumStreams,
					 int64 blockFirstRowNum)
{
	int			i;

	Assert(datumStreams != NULL);

	for (i = 0; i < numDatumStreams; i++)
	{
		Assert(datumStreams[i] != NULL);

		datumStreams[i]->blockFirstRowNum = blockFirstRowNum;
	}
}


AOCSInsertDesc
aocs_insert_init(Relation rel, int segno, bool update_mode)
{
	AOCSInsertDesc desc;
	TupleDesc	tupleDesc;
	int64		firstSequence = 0;

	desc = (AOCSInsertDesc) palloc0(sizeof(AOCSInsertDescData));
	desc->aoi_rel = rel;
	desc->appendOnlyMetaDataSnapshot = SnapshotSelf;

#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("ao_column_insert_init_1") == FaultInjectorTypeSkip)
	{
		SIMPLE_FAULT_INJECTOR("ao_column_insert_init_2");
	}
#endif

	/*
	 * Writers uses this since they have exclusive access to the lock acquired
	 * with LockRelationAppendOnlySegmentFile for the segment-file.
	 */

	tupleDesc = RelationGetDescr(desc->aoi_rel);

	Assert(segno >= 0);
	desc->cur_segno = segno;
	desc->update_mode = update_mode;

	desc->compLevel = rel->rd_appendonly->compresslevel;
	desc->compType = NameStr(rel->rd_appendonly->compresstype);
	desc->blocksz = rel->rd_appendonly->blocksize;

	OpenAOCSDatumStreams(desc);

	/*
	 * Obtain the next list of fast sequences for this relation.
	 *
	 * Even in the case of no indexes, we need to update the fast sequences,
	 * since the table may contain indexes at some point of time.
	 */
	desc->numSequences = 0;

	firstSequence =
		GetFastSequences(rel->rd_appendonly->segrelid,
						 segno,
						 desc->rowCount + 1,
						 NUM_FAST_SEQUENCES);
	desc->numSequences = NUM_FAST_SEQUENCES;

	/* Set last_sequence value */
	Assert(firstSequence > desc->rowCount);
	desc->lastSequence = firstSequence - 1;

	SetBlockFirstRowNums(desc->ds, tupleDesc->natts, desc->lastSequence + 1);

	/* Initialize the block directory. */
	tupleDesc = RelationGetDescr(rel);
	AppendOnlyBlockDirectory_Init_forInsert(&(desc->blockDirectory),
											desc->appendOnlyMetaDataSnapshot,	/* CONCERN: Safe to
																				 * assume all block
																				 * directory entries for
																				 * segment are "covered"
																				 * by same exclusive
																				 * lock. */
											(FileSegInfo *) desc->fsInfo, desc->lastSequence,
											rel, segno, tupleDesc->natts, true);

	return desc;
}


Oid
aocs_insert_values(AOCSInsertDesc idesc, Datum *d, bool *null, AOTupleId *aoTupleId)
{
	Relation	rel = idesc->aoi_rel;
	int			i;

	if (rel->rd_rel->relhasoids)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("append-only column-oriented tables do not support rows with OIDs")));

#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet(
								   "appendonly_insert",
								   DDLNotSpecified,
								   "",	/* databaseName */
								   RelationGetRelationName(idesc->aoi_rel));	/* tableName */
#endif

	/*
	 * Generate new transaction id if necessary, so dependent entities, such as
	 * spgist indexes, can use it outside aocs_insert_values.
	 */
	(void) GetCurrentTransactionId();

	/* As usual, at this moment, we assume one col per vp */
	for (i = 0; i < RelationGetNumberOfAttributes(rel); ++i)
	{
		void	   *toFree1;
		Datum		datum = d[i];
		int			err = datumstreamwrite_put(idesc->ds[i], datum, null[i], &toFree1);

		if (toFree1 != NULL)
		{
			/*
			 * Use the de-toasted and/or de-compressed as datum instead.
			 */
			datum = PointerGetDatum(toFree1);
		}
		if (err < 0)
		{
			int			itemCount = datumstreamwrite_nth(idesc->ds[i]);
			void	   *toFree2;

			/* write the block up to this one */
			datumstreamwrite_block(idesc->ds[i], &idesc->blockDirectory, i, false);
			if (itemCount > 0)
			{
				/*
				 * since we have written all up to the new tuple, the new
				 * blockFirstRowNum is the inserted tuple's row number
				 */
				idesc->ds[i]->blockFirstRowNum = idesc->lastSequence + 1;
			}

			Assert(idesc->ds[i]->blockFirstRowNum == idesc->lastSequence + 1);


			/* now write this new item to the new block */
			err = datumstreamwrite_put(idesc->ds[i], datum, null[i], &toFree2);
			Assert(toFree2 == NULL);
			if (err < 0)
			{
				Assert(!null[i]);
				err = datumstreamwrite_lob(idesc->ds[i],
										   datum,
										   &idesc->blockDirectory,
										   i,
										   false);
				Assert(err >= 0);

				/*
				 * A lob will live by itself in the block so this assignment
				 * is for the block that contains tuples AFTER the one we are
				 * inserting
				 */
				idesc->ds[i]->blockFirstRowNum = idesc->lastSequence + 2;
			}
		}

		if (toFree1 != NULL)
			pfree(toFree1);
	}

	idesc->insertCount++;
	idesc->lastSequence++;
	if (idesc->numSequences > 0)
		(idesc->numSequences)--;

	if (!idesc->update_mode)
		pgstat_count_heap_insert(rel, 1);
	else
		pgstat_count_heap_update(rel, false);

	Assert(idesc->numSequences >= 0);

	AOTupleIdInit(aoTupleId, idesc->cur_segno, idesc->lastSequence);

	/*
	 * If the allocated fast sequence numbers are used up, we request for a
	 * next list of fast sequence numbers.
	 */
	if (idesc->numSequences == 0)
	{
		int64		firstSequence;

		firstSequence =
			GetFastSequences(rel->rd_appendonly->segrelid,
							 idesc->cur_segno,
							 idesc->lastSequence + 1,
							 NUM_FAST_SEQUENCES);

		Assert(firstSequence == idesc->lastSequence + 1);
		idesc->numSequences = NUM_FAST_SEQUENCES;
	}

	return InvalidOid;
}

void
aocs_insert_finish(AOCSInsertDesc idesc)
{
	Relation	rel = idesc->aoi_rel;
	int			i;

	for (i = 0; i < rel->rd_att->natts; ++i)
	{
		datumstreamwrite_block(idesc->ds[i], &idesc->blockDirectory, i, false);
		datumstreamwrite_close_file(idesc->ds[i]);
	}

	AppendOnlyBlockDirectory_End_forInsert(&(idesc->blockDirectory));

	UpdateAOCSFileSegInfo(idesc);

	pfree(idesc->fsInfo);

	close_ds_write(idesc->ds, rel->rd_att->natts);
}

static void
positionFirstBlockOfRange(DatumStreamFetchDesc datumStreamFetchDesc)
{
	AppendOnlyBlockDirectoryEntry_GetBeginRange(
												&datumStreamFetchDesc->currentBlock.blockDirectoryEntry,
												&datumStreamFetchDesc->scanNextFileOffset,
												&datumStreamFetchDesc->scanNextRowNum);
}

static void
positionLimitToEndOfRange(DatumStreamFetchDesc datumStreamFetchDesc)
{
	AppendOnlyBlockDirectoryEntry_GetEndRange(
											  &datumStreamFetchDesc->currentBlock.blockDirectoryEntry,
											  &datumStreamFetchDesc->scanAfterFileOffset,
											  &datumStreamFetchDesc->scanLastRowNum);
}


static void
positionSkipCurrentBlock(DatumStreamFetchDesc datumStreamFetchDesc)
{
	datumStreamFetchDesc->scanNextFileOffset =
		datumStreamFetchDesc->currentBlock.fileOffset +
		datumStreamFetchDesc->currentBlock.overallBlockLen;

	datumStreamFetchDesc->scanNextRowNum =
		datumStreamFetchDesc->currentBlock.lastRowNum + 1;
}

static void
fetchFromCurrentBlock(AOCSFetchDesc aocsFetchDesc,
					  int64 rowNum,
					  TupleTableSlot *slot,
					  int colno)
{
	DatumStreamFetchDesc datumStreamFetchDesc =
	aocsFetchDesc->datumStreamFetchDesc[colno];
	DatumStreamRead *datumStream = datumStreamFetchDesc->datumStream;
	Datum		value;
	bool		null;
	int			rowNumInBlock = rowNum - datumStreamFetchDesc->currentBlock.firstRowNum;

	Assert(rowNumInBlock >= 0);

	/*
	 * MPP-17061: gotContents could be false in the case of aborted rows. As
	 * described in the repro in MPP-17061, if aocs_fetch is trying to fetch
	 * an invisible/aborted row, it could set the block header metadata of
	 * currentBlock to the next CO block, but without actually reading in next
	 * CO block's content.
	 */
	if (datumStreamFetchDesc->currentBlock.gotContents == false)
	{
		datumstreamread_block_content(datumStream);
		datumStreamFetchDesc->currentBlock.gotContents = true;
	}

	datumstreamread_find(datumStream, rowNumInBlock);

	if (slot != NULL)
	{
		Datum	   *values = slot_get_values(slot);
		bool	   *nulls = slot_get_isnull(slot);
		int			formatversion = datumStream->ao_read.formatVersion;

		datumstreamread_get(datumStream, &(values[colno]), &(nulls[colno]));

		/*
		 * Perform any required upgrades on the Datum we just fetched.
		 */
		if (formatversion < AORelationVersion_GetLatest())
		{
			upgrade_datum_fetch(aocsFetchDesc, colno, values, nulls,
								formatversion);
		}
	}
	else
	{
		datumstreamread_get(datumStream, &value, &null);
	}
}

static bool
scanToFetchValue(AOCSFetchDesc aocsFetchDesc,
				 int64 rowNum,
				 TupleTableSlot *slot,
				 int colno)
{
	DatumStreamFetchDesc datumStreamFetchDesc = aocsFetchDesc->datumStreamFetchDesc[colno];
	DatumStreamRead *datumStream = datumStreamFetchDesc->datumStream;
	bool		found;

	found = datumstreamread_find_block(datumStream,
									   datumStreamFetchDesc,
									   rowNum);
	if (found)
		fetchFromCurrentBlock(aocsFetchDesc, rowNum, slot, colno);

	return found;
}

static void
closeFetchSegmentFile(DatumStreamFetchDesc datumStreamFetchDesc)
{
	Assert(datumStreamFetchDesc->currentSegmentFile.isOpen);

	datumstreamread_close_file(datumStreamFetchDesc->datumStream);
	datumStreamFetchDesc->currentSegmentFile.isOpen = false;
}

static bool
openFetchSegmentFile(AOCSFetchDesc aocsFetchDesc,
					 int openSegmentFileNum,
					 int colNo)
{
	int			i;

	AOCSFileSegInfo *fsInfo;
	int			segmentFileNum;
	int64		logicalEof;
	DatumStreamFetchDesc datumStreamFetchDesc = aocsFetchDesc->datumStreamFetchDesc[colNo];

	Assert(!datumStreamFetchDesc->currentSegmentFile.isOpen);

	i = 0;
	while (true)
	{
		if (i >= aocsFetchDesc->totalSegfiles)
			return false;
		/* Segment file not visible in catalog information. */

		fsInfo = aocsFetchDesc->segmentFileInfo[i];
		segmentFileNum = fsInfo->segno;
		if (openSegmentFileNum == segmentFileNum)
		{
			AOCSVPInfoEntry *entry = getAOCSVPEntry(fsInfo, colNo);

			logicalEof = entry->eof;
			break;
		}
		i++;
	}

	/*
	 * Don't try to open a segment file when its EOF is 0, since the file may
	 * not exist. See MPP-8280. Also skip the segment file if it is awaiting a
	 * drop
	 */
	if (logicalEof == 0 || fsInfo->state == AOSEG_STATE_AWAITING_DROP)
		return false;

	open_datumstreamread_segfile(aocsFetchDesc->basepath, aocsFetchDesc->relation->rd_node,
								 fsInfo,
								 datumStreamFetchDesc->datumStream,
								 colNo);

	datumStreamFetchDesc->currentSegmentFile.num = openSegmentFileNum;
	datumStreamFetchDesc->currentSegmentFile.logicalEof = logicalEof;

	datumStreamFetchDesc->currentSegmentFile.isOpen = true;

	return true;
}

static void
resetCurrentBlockInfo(CurrentBlock *currentBlock)
{
	currentBlock->have = false;
	currentBlock->firstRowNum = 0;
	currentBlock->lastRowNum = 0;
}

/*
 * Initialize the fetch descriptor.
 */
AOCSFetchDesc
aocs_fetch_init(Relation relation,
				Snapshot snapshot,
				Snapshot appendOnlyMetaDataSnapshot,
				bool *proj)
{
	AOCSFetchDesc aocsFetchDesc;
	int			colno;
	char	   *basePath = relpathbackend(relation->rd_node, relation->rd_backend, MAIN_FORKNUM);
	TupleDesc	tupleDesc = RelationGetDescr(relation);
	StdRdOptions **opts = RelationGetAttributeOptions(relation);

	/*
	 * increment relation ref count while scanning relation
	 *
	 * This is just to make really sure the relcache entry won't go away while
	 * the scan has a pointer to it.  Caller should be holding the rel open
	 * anyway, so this is redundant in all normal scenarios...
	 */
	RelationIncrementReferenceCount(relation);

	aocsFetchDesc = (AOCSFetchDesc) palloc0(sizeof(AOCSFetchDescData));
	aocsFetchDesc->relation = relation;

	aocsFetchDesc->appendOnlyMetaDataSnapshot = appendOnlyMetaDataSnapshot;
	aocsFetchDesc->snapshot = snapshot;


	aocsFetchDesc->initContext = CurrentMemoryContext;

	aocsFetchDesc->segmentFileNameMaxLen = AOSegmentFilePathNameLen(relation) + 1;
	aocsFetchDesc->segmentFileName =
		(char *) palloc(aocsFetchDesc->segmentFileNameMaxLen);
	aocsFetchDesc->segmentFileName[0] = '\0';
	aocsFetchDesc->basepath = basePath;

	Assert(proj);

	aocsFetchDesc->segmentFileInfo =
		GetAllAOCSFileSegInfo(relation, appendOnlyMetaDataSnapshot, &aocsFetchDesc->totalSegfiles);

	AppendOnlyBlockDirectory_Init_forSearch(
											&aocsFetchDesc->blockDirectory,
											appendOnlyMetaDataSnapshot,
											(FileSegInfo **) aocsFetchDesc->segmentFileInfo,
											aocsFetchDesc->totalSegfiles,
											aocsFetchDesc->relation,
											relation->rd_att->natts,
											true,
											proj);

	Assert(relation->rd_att != NULL);

	aocsFetchDesc->datumStreamFetchDesc = (DatumStreamFetchDesc *)
		palloc0(relation->rd_att->natts * sizeof(DatumStreamFetchDesc));

	for (colno = 0; colno < relation->rd_att->natts; colno++)
	{

		aocsFetchDesc->datumStreamFetchDesc[colno] = NULL;
		if (proj[colno])
		{
			char	   *ct;
			int32		clvl;
			int32		blksz;

			StringInfoData titleBuf;

			/*
			 * We always record all the three column specific attributes for
			 * each column of a column oriented table. Note: checksum is a
			 * table level attribute.
			 */
			Assert(opts[colno]);
			ct = opts[colno]->compresstype;
			clvl = opts[colno]->compresslevel;
			blksz = opts[colno]->blocksize;

			/* UNDONE: Need to track and dispose of this storage... */
			initStringInfo(&titleBuf);
			appendStringInfo(&titleBuf, "Fetch from Append-Only Column-Oriented relation '%s', column #%d '%s'",
							 RelationGetRelationName(relation),
							 colno + 1,
							 NameStr(tupleDesc->attrs[colno]->attname));

			aocsFetchDesc->datumStreamFetchDesc[colno] = (DatumStreamFetchDesc)
				palloc0(sizeof(DatumStreamFetchDescData));

			
			HeapTuple tp;
			char * nspname;

			tp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(relation->rd_rel->relnamespace));

			if (HeapTupleIsValid(tp))
			{
				Form_pg_namespace nsptup = (Form_pg_namespace) GETSTRUCT(tp);
				nspname = pstrdup(NameStr(nsptup->nspname));
				ReleaseSysCache(tp);
			} else {
				elog(ERROR, "yezzey: failed to get namescape name of relation %s", relation->rd_rel->relname.data);
			}


			aocsFetchDesc->datumStreamFetchDesc[colno]->datumStream =
				create_datumstreamread(ct,
									   clvl,
									   relation->rd_appendonly->checksum,
									    /* safeFSWriteSize */ false,	/* UNDONE:Need to wire
																		 * down pg_appendonly
																		 * column */
									   blksz,
									   tupleDesc->attrs[colno],
									   RelationGetRelationName(relation),
									   nspname,
									   relation->rd_id,
									    /* title */ titleBuf.data);


			pfree(nspname);
		}
		if (opts[colno])
			pfree(opts[colno]);
	}
	if (opts)
		pfree(opts);
	AppendOnlyVisimap_Init(&aocsFetchDesc->visibilityMap,
						   relation->rd_appendonly->visimaprelid,
						   relation->rd_appendonly->visimapidxid,
						   AccessShareLock,
						   appendOnlyMetaDataSnapshot);

	return aocsFetchDesc;
}

/*
 * Fetch the tuple based on the given tuple id.
 *
 * If the 'slot' is not NULL, the tuple will be assigned to the slot.
 *
 * Return true if the tuple is found. Otherwise, return false.
 */
bool
aocs_fetch(AOCSFetchDesc aocsFetchDesc,
		   AOTupleId *aoTupleId,
		   TupleTableSlot *slot)
{
	int			segmentFileNum = AOTupleIdGet_segmentFileNum(aoTupleId);
	int64		rowNum = AOTupleIdGet_rowNum(aoTupleId);
	int			numCols = aocsFetchDesc->relation->rd_att->natts;
	int			colno;
	bool		found = true;
	bool		isSnapshotAny = (aocsFetchDesc->snapshot == SnapshotAny);

	Assert(numCols > 0);

	/*
	 * Go through columns one by one. Check if the current block has the
	 * requested tuple. If so, fetch it. Otherwise, read the block that
	 * contains the requested tuple.
	 */
	for (colno = 0; colno < numCols; colno++)
	{
		DatumStreamFetchDesc datumStreamFetchDesc = aocsFetchDesc->datumStreamFetchDesc[colno];

		/* If this column does not need to be fetched, skip it. */
		if (datumStreamFetchDesc == NULL)
			continue;

		elogif(Debug_appendonly_print_datumstream, LOG,
			   "aocs_fetch filePathName %s segno %u rowNum  " INT64_FORMAT
			   " firstRowNum " INT64_FORMAT " lastRowNum " INT64_FORMAT " ",
			   datumStreamFetchDesc->datumStream->ao_read.bufferedRead.filePathName,
			   datumStreamFetchDesc->currentSegmentFile.num,
			   rowNum,
			   datumStreamFetchDesc->currentBlock.firstRowNum,
			   datumStreamFetchDesc->currentBlock.lastRowNum);

		/*
		 * If the current block has the requested tuple, read it.
		 */
		if (datumStreamFetchDesc->currentSegmentFile.isOpen &&
			datumStreamFetchDesc->currentSegmentFile.num == segmentFileNum &&
			aocsFetchDesc->blockDirectory.currentSegmentFileNum == segmentFileNum &&
			datumStreamFetchDesc->currentBlock.have)
		{
			if (rowNum >= datumStreamFetchDesc->currentBlock.firstRowNum &&
				rowNum <= datumStreamFetchDesc->currentBlock.lastRowNum)
			{
				if (!isSnapshotAny && !AppendOnlyVisimap_IsVisible(&aocsFetchDesc->visibilityMap, aoTupleId))
				{
					found = false;
					break;
				}

				fetchFromCurrentBlock(aocsFetchDesc, rowNum, slot, colno);
				continue;
			}

			/*
			 * Otherwise, fetch the right block.
			 */
			if (AppendOnlyBlockDirectoryEntry_RangeHasRow(
														  &(datumStreamFetchDesc->currentBlock.blockDirectoryEntry),
														  rowNum))
			{
				/*
				 * The tuple is covered by the current Block Directory entry,
				 * but is it before or after our current block?
				 */
				if (rowNum < datumStreamFetchDesc->currentBlock.firstRowNum)
				{
					/*
					 * Set scan range to prior block
					 */
					positionFirstBlockOfRange(datumStreamFetchDesc);

					datumStreamFetchDesc->scanAfterFileOffset =
						datumStreamFetchDesc->currentBlock.fileOffset;
					datumStreamFetchDesc->scanLastRowNum =
						datumStreamFetchDesc->currentBlock.firstRowNum - 1;
				}
				else
				{
					/*
					 * Set scan range to following blocks.
					 */
					positionSkipCurrentBlock(datumStreamFetchDesc);
					positionLimitToEndOfRange(datumStreamFetchDesc);
				}

				if (!isSnapshotAny && !AppendOnlyVisimap_IsVisible(&aocsFetchDesc->visibilityMap, aoTupleId))
				{
					found = false;
					break;
				}

				if (!scanToFetchValue(aocsFetchDesc, rowNum, slot, colno))
				{
					found = false;
					break;
				}

				continue;
			}
		}

		/*
		 * Open or switch open, if necessary.
		 */
		if (datumStreamFetchDesc->currentSegmentFile.isOpen &&
			segmentFileNum != datumStreamFetchDesc->currentSegmentFile.num)
		{
			closeFetchSegmentFile(datumStreamFetchDesc);

			Assert(!datumStreamFetchDesc->currentSegmentFile.isOpen);
		}

		if (!datumStreamFetchDesc->currentSegmentFile.isOpen)
		{
			if (!openFetchSegmentFile(aocsFetchDesc,
									  segmentFileNum,
									  colno))
			{
				found = false;
				/* Segment file not in aoseg table.. */
				/* Must be aborted or deleted and reclaimed. */
				break;
			}

			/* Reset currentBlock info */
			resetCurrentBlockInfo(&(datumStreamFetchDesc->currentBlock));
		}

		/*
		 * Need to get the Block Directory entry that covers the TID.
		 */
		if (!AppendOnlyBlockDirectory_GetEntry(&aocsFetchDesc->blockDirectory,
											   aoTupleId,
											   colno,
											   &datumStreamFetchDesc->currentBlock.blockDirectoryEntry))
		{
			found = false;		/* Row not represented in Block Directory. */
			/* Must be aborted or deleted and reclaimed. */
			break;
		}

		if (!isSnapshotAny && !AppendOnlyVisimap_IsVisible(&aocsFetchDesc->visibilityMap, aoTupleId))
		{
			found = false;
			break;
		}

		/*
		 * Set scan range covered by new Block Directory entry.
		 */
		positionFirstBlockOfRange(datumStreamFetchDesc);

		positionLimitToEndOfRange(datumStreamFetchDesc);

		if (!scanToFetchValue(aocsFetchDesc, rowNum, slot, colno))
		{
			found = false;
			break;
		}
	}

	if (found)
	{
		if (slot != NULL)
		{
			TupSetVirtualTupleNValid(slot, colno);
			slot_set_ctid(slot, (ItemPointer) aoTupleId);
		}
	}
	else
	{
		if (slot != NULL)
			slot = ExecClearTuple(slot);
	}

	return found;
}

void
aocs_fetch_finish(AOCSFetchDesc aocsFetchDesc)
{
	int			colno;
	Relation	relation = aocsFetchDesc->relation;

	Assert(relation != NULL && relation->rd_att != NULL);

	for (colno = 0; colno < relation->rd_att->natts; colno++)
	{
		DatumStreamFetchDesc datumStreamFetchDesc = aocsFetchDesc->datumStreamFetchDesc[colno];

		if (datumStreamFetchDesc != NULL)
		{
			Assert(datumStreamFetchDesc->datumStream != NULL);
			datumstreamread_close_file(datumStreamFetchDesc->datumStream);
			destroy_datumstreamread(datumStreamFetchDesc->datumStream);
			datumStreamFetchDesc->datumStream = NULL;
			pfree(datumStreamFetchDesc);
			aocsFetchDesc->datumStreamFetchDesc[colno] = NULL;
		}
	}
	pfree(aocsFetchDesc->datumStreamFetchDesc);

	AppendOnlyBlockDirectory_End_forSearch(&aocsFetchDesc->blockDirectory);

	if (aocsFetchDesc->segmentFileInfo)
	{
		FreeAllAOCSSegFileInfo(aocsFetchDesc->segmentFileInfo, aocsFetchDesc->totalSegfiles);
		pfree(aocsFetchDesc->segmentFileInfo);
		aocsFetchDesc->segmentFileInfo = NULL;
	}

	RelationDecrementReferenceCount(aocsFetchDesc->relation);

	pfree(aocsFetchDesc->segmentFileName);
	pfree(aocsFetchDesc->basepath);

	AppendOnlyVisimap_Finish(&aocsFetchDesc->visibilityMap, AccessShareLock);
}

typedef struct AOCSUpdateDescData
{
	AOCSInsertDesc insertDesc;

	/*
	 * visibility map
	 */
	AppendOnlyVisimap visibilityMap;

	/*
	 * Visimap delete support structure. Used to handle out-of-order deletes
	 */
	AppendOnlyVisimapDelete visiMapDelete;

}			AOCSUpdateDescData;

AOCSUpdateDesc
aocs_update_init(Relation rel, int segno)
{
	AOCSUpdateDesc desc = (AOCSUpdateDesc) palloc0(sizeof(AOCSUpdateDescData));

	desc->insertDesc = aocs_insert_init(rel, segno, true);

	AppendOnlyVisimap_Init(&desc->visibilityMap,
						   rel->rd_appendonly->visimaprelid,
						   rel->rd_appendonly->visimapidxid,
						   RowExclusiveLock,
						   desc->insertDesc->appendOnlyMetaDataSnapshot);

	AppendOnlyVisimapDelete_Init(&desc->visiMapDelete,
								 &desc->visibilityMap);

	return desc;
}

void
aocs_update_finish(AOCSUpdateDesc desc)
{
	Assert(desc);

	AppendOnlyVisimapDelete_Finish(&desc->visiMapDelete);

	aocs_insert_finish(desc->insertDesc);
	desc->insertDesc = NULL;

	/* Keep lock until the end of transaction */
	AppendOnlyVisimap_Finish(&desc->visibilityMap, NoLock);

	pfree(desc);
}

HTSU_Result
aocs_update(AOCSUpdateDesc desc, TupleTableSlot *slot,
			AOTupleId *oldTupleId, AOTupleId *newTupleId)
{
	Oid oid;
	HTSU_Result result;

	Assert(desc);
	Assert(oldTupleId);
	Assert(newTupleId);

#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet(
								   "appendonly_update",
								   DDLNotSpecified,
								   "", //databaseName
								   RelationGetRelationName(desc->insertDesc->aoi_rel));
	/* tableName */
#endif

	result = AppendOnlyVisimapDelete_Hide(&desc->visiMapDelete, oldTupleId);
	if (result != HeapTupleMayBeUpdated)
		return result;

	slot_getallattrs(slot);
	oid = aocs_insert_values(desc->insertDesc,
							 slot_get_values(slot), slot_get_isnull(slot),
							 newTupleId);
	(void) oid;					/* ignore the oid value */

	return result;
}


/*
 * AOCSDeleteDescData is used for delete data from AOCS relations.
 * It serves an equivalent purpose as AppendOnlyScanDescData
 * (relscan.h) only that the later is used for scanning append-only
 * relations.
 */
typedef struct AOCSDeleteDescData
{
	/*
	 * Relation to delete from
	 */
	Relation	aod_rel;

	/*
	 * visibility map
	 */
	AppendOnlyVisimap visibilityMap;

	/*
	 * Visimap delete support structure. Used to handle out-of-order deletes
	 */
	AppendOnlyVisimapDelete visiMapDelete;

}			AOCSDeleteDescData;


/*
 * appendonly_delete_init
 *
 * before using appendonly_delete() to delete tuples from append-only segment
 * files, we need to call this function to initialize the delete desc
 * data structured.
 */
AOCSDeleteDesc
aocs_delete_init(Relation rel)
{
	/*
	 * Get the pg_appendonly information
	 */
	AOCSDeleteDesc aoDeleteDesc = palloc0(sizeof(AOCSDeleteDescData));

	aoDeleteDesc->aod_rel = rel;

	AppendOnlyVisimap_Init(&aoDeleteDesc->visibilityMap,
						   rel->rd_appendonly->visimaprelid,
						   rel->rd_appendonly->visimapidxid,
						   RowExclusiveLock,
						   GetCatalogSnapshot(InvalidOid));

	AppendOnlyVisimapDelete_Init(&aoDeleteDesc->visiMapDelete,
								 &aoDeleteDesc->visibilityMap);

	return aoDeleteDesc;
}

void
aocs_delete_finish(AOCSDeleteDesc aoDeleteDesc)
{
	Assert(aoDeleteDesc);

	AppendOnlyVisimapDelete_Finish(&aoDeleteDesc->visiMapDelete);
	AppendOnlyVisimap_Finish(&aoDeleteDesc->visibilityMap, NoLock);

	pfree(aoDeleteDesc);
}

HTSU_Result
aocs_delete(AOCSDeleteDesc aoDeleteDesc,
			AOTupleId *aoTupleId)
{
	Assert(aoDeleteDesc);
	Assert(aoTupleId);

	elogif(Debug_appendonly_print_delete, LOG,
		   "AOCS delete tuple from table '%s' (AOTupleId %s)",
		   NameStr(aoDeleteDesc->aod_rel->rd_rel->relname),
		   AOTupleIdToString(aoTupleId));

#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet(
								   "appendonly_delete",
								   DDLNotSpecified,
								   "",	/* databaseName */
								   RelationGetRelationName(aoDeleteDesc->aod_rel)); /* tableName */
#endif

	pgstat_count_heap_delete(aoDeleteDesc->aod_rel);

	return AppendOnlyVisimapDelete_Hide(&aoDeleteDesc->visiMapDelete, aoTupleId);
}

/*
 * Initialize a scan on varblock headers in an AOCS segfile.  The
 * segfile is identified by colno.
 */
AOCSHeaderScanDesc
aocs_begin_headerscan(Relation rel, int colno)
{
	AOCSHeaderScanDesc hdesc;
	AppendOnlyStorageAttributes ao_attr;
	StdRdOptions **opts = RelationGetAttributeOptions(rel);
	HeapTuple tp;
	char * nspname;

	Assert(opts[colno]);

	ao_attr.checksum = rel->rd_appendonly->checksum;

	/*
	 * We are concerned with varblock headers only, not their content.
	 * Therefore, don't waste cycles in decompressing the content.
	 */
	ao_attr.compress = false;
	ao_attr.compressType = NULL;
	ao_attr.compressLevel = 0;
	ao_attr.overflowSize = 0;
	ao_attr.safeFSWriteSize = 0;
	hdesc = palloc(sizeof(AOCSHeaderScanDescData));

	tp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(rel->rd_rel->relnamespace));

	if (HeapTupleIsValid(tp))
	{
		Form_pg_namespace nsptup = (Form_pg_namespace) GETSTRUCT(tp);
		nspname = pstrdup(NameStr(nsptup->nspname));
		ReleaseSysCache(tp);
	} else {
		elog(ERROR, "yezzey: failed to get namescape name of relation %s", rel->rd_rel->relname.data);
	}


	AppendOnlyStorageRead_Init(
							   &hdesc->ao_read,
							   rel->rd_id,
							   NULL, //current memory context
							   opts[colno]->blocksize,
							   nspname,
							   RelationGetRelationName(rel),
							   "ALTER TABLE ADD COLUMN scan",
							   &ao_attr);
	hdesc->colno = colno;

	for (int i = 0; i < RelationGetNumberOfAttributes(rel); i++)
		pfree(opts[i]);
	pfree(opts);
	pfree(nspname);

	return hdesc;
}

/*
 * Open AOCS segfile for scanning varblock headers.
 */
void
aocs_headerscan_opensegfile(AOCSHeaderScanDesc hdesc,
							AOCSFileSegInfo *seginfo,
							char *basepath, RelFileNode rnode)
{
	AOCSVPInfoEntry *vpe;
	char		fn[MAXPGPATH];
	int32		fileSegNo;

	/* Close currently open segfile, if any. */
	AppendOnlyStorageRead_CloseFile(&hdesc->ao_read);
	FormatAOSegmentFileName(basepath, seginfo->segno,
							hdesc->colno, &fileSegNo, fn);
	Assert(strlen(fn) + 1 <= MAXPGPATH);
	vpe = getAOCSVPEntry(seginfo, hdesc->colno);
	AppendOnlyStorageRead_OpenFile(&hdesc->ao_read, fn, seginfo->formatversion,
								   vpe->eof, rnode);
}

bool
aocs_get_nextheader(AOCSHeaderScanDesc hdesc)
{
	if (hdesc->ao_read.current.firstRowNum > 0)
		AppendOnlyStorageRead_SkipCurrentBlock(&hdesc->ao_read);

	return AppendOnlyStorageRead_ReadNextBlock(&hdesc->ao_read);
}

void
aocs_end_headerscan(AOCSHeaderScanDesc hdesc)
{
	AppendOnlyStorageRead_CloseFile(&hdesc->ao_read);
	AppendOnlyStorageRead_FinishSession(&hdesc->ao_read);
	pfree(hdesc);
}

/*
 * Initialize one datum stream per new column for writing.
 */
AOCSAddColumnDesc
aocs_addcol_init(Relation rel,
				 int num_newcols)
{
	char	   *ct;
	int32		clvl;
	int32		blksz;
	AOCSAddColumnDesc desc;
	int			i;
	int			iattr;
	StringInfoData titleBuf;

	desc = palloc(sizeof(AOCSAddColumnDescData));
	desc->num_newcols = num_newcols;
	desc->rel = rel;
	desc->cur_segno = -1;

	/*
	 * Rewrite catalog phase of alter table has updated catalog with info for
	 * new columns, which is available through rel.
	 */
	StdRdOptions **opts = RelationGetAttributeOptions(rel);

	desc->dsw = palloc(sizeof(DatumStreamWrite *) * desc->num_newcols);

	iattr = rel->rd_att->natts - num_newcols;
	for (i = 0; i < num_newcols; ++i, ++iattr)
	{
		Form_pg_attribute attr = rel->rd_att->attrs[iattr];

		initStringInfo(&titleBuf);
		appendStringInfo(&titleBuf, "ALTER TABLE ADD COLUMN new segfile");

		Assert(opts[iattr]);
		ct = opts[iattr]->compresstype;
		clvl = opts[iattr]->compresslevel;
		blksz = opts[iattr]->blocksize;
		HeapTuple tp;
		char * nspname;

		tp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(rel->rd_rel->relnamespace));

		if (HeapTupleIsValid(tp))
		{
			Form_pg_namespace nsptup = (Form_pg_namespace) GETSTRUCT(tp);
			nspname = pstrdup(NameStr(nsptup->nspname));
			ReleaseSysCache(tp);
		} else {
			elog(ERROR, "yezzey: failed to get namescape name of relation %s", rel->rd_rel->relname.data);
		}


		desc->dsw[i] = create_datumstreamwrite(ct, clvl, rel->rd_appendonly->checksum, 0, blksz /* safeFSWriteSize */ ,
											   attr, nspname, RelationGetRelationName(rel),
											   RelationGetRelid(rel),
											   titleBuf.data,
											   XLogIsNeeded() && RelationNeedsWAL(rel));



		pfree(nspname);
	}

	for (i = 0; i < RelationGetNumberOfAttributes(rel); i++)
			pfree(opts[i]);
	pfree(opts);

	return desc;
}

/*
 * Create new physical segfiles for each newly added column.
 */
void
aocs_addcol_newsegfile(AOCSAddColumnDesc desc,
					   AOCSFileSegInfo *seginfo,
					   char *basepath,
					   RelFileNodeBackend relfilenode)
{
	int32		fileSegNo;
	char		fn[MAXPGPATH];
	int			i;
	Snapshot	appendOnlyMetaDataSnapshot = RegisterSnapshot(GetCatalogSnapshot(InvalidOid));

	/* Column numbers of newly added columns start from here. */
	AttrNumber	colno = desc->rel->rd_att->natts - desc->num_newcols;

	if (desc->dsw[0]->need_close_file)
	{
		aocs_addcol_closefiles(desc);
		AppendOnlyBlockDirectory_End_addCol(&desc->blockDirectory);
	}
	AppendOnlyBlockDirectory_Init_addCol(&desc->blockDirectory,
										 appendOnlyMetaDataSnapshot,
										 (FileSegInfo *) seginfo,
										 desc->rel,
										 seginfo->segno,
										 desc->num_newcols,
										 true /* isAOCol */ );
	for (i = 0; i < desc->num_newcols; ++i, ++colno)
	{
		int			version;

		/* Always write in the latest format */
		version = AORelationVersion_GetLatest();

		FormatAOSegmentFileName(basepath, seginfo->segno, colno,
								&fileSegNo, fn);
		Assert(strlen(fn) + 1 <= MAXPGPATH);
		datumstreamwrite_open_file(desc->dsw[i], fn,
								   0 /* eof */ , 0 /* eof_uncompressed */ , 0, /*modcount*/
								   &relfilenode, fileSegNo,
								   version);
		desc->dsw[i]->blockFirstRowNum = 1;
	}
	desc->cur_segno = seginfo->segno;
	UnregisterSnapshot(appendOnlyMetaDataSnapshot);
}

void
aocs_addcol_closefiles(AOCSAddColumnDesc desc)
{
	int			i;
	AttrNumber	colno = desc->rel->rd_att->natts - desc->num_newcols;

	for (i = 0; i < desc->num_newcols; ++i)
	{
		datumstreamwrite_block(desc->dsw[i], &desc->blockDirectory, i + colno, true);
		datumstreamwrite_close_file(desc->dsw[i]);
	}
	/* Update pg_aocsseg_* with eof of each segfile we just closed. */
	AOCSFileSegInfoAddVpe(desc->rel, desc->cur_segno, desc,
						  desc->num_newcols, false /* non-empty VPEntry */ );
}

void
aocs_addcol_setfirstrownum(AOCSAddColumnDesc desc, int64 firstRowNum)
{
       int                     i;
       for (i = 0; i < desc->num_newcols; ++i)
       {
               /*
                * Next block's first row number.
                */
               desc->dsw[i]->blockFirstRowNum = firstRowNum;
       }
}


/*
 * Force writing new varblock in each segfile open for insert.
 */
void
aocs_addcol_endblock(AOCSAddColumnDesc desc, int64 firstRowNum)
{
	int			i;
	AttrNumber	colno = desc->rel->rd_att->natts - desc->num_newcols;

	for (i = 0; i < desc->num_newcols; ++i)
	{
		datumstreamwrite_block(desc->dsw[i], &desc->blockDirectory, i + colno, true);

		/*
		 * Next block's first row number.  In this case, the block being ended
		 * has less number of rows than its capacity.
		 */
		desc->dsw[i]->blockFirstRowNum = firstRowNum;
	}
}

/*
 * Insert one new datum for each new column being added.  This is
 * derived from aocs_insert_values().
 */
void
aocs_addcol_insert_datum(AOCSAddColumnDesc desc, Datum *d, bool *isnull)
{
	void	   *toFree1;
	void	   *toFree2;
	Datum		datum;
	int			err;
	int			i;
	int			itemCount;

	/* first column's number */
	AttrNumber	colno = desc->rel->rd_att->natts - desc->num_newcols;

	for (i = 0; i < desc->num_newcols; ++i)
	{
		datum = d[i];
		err = datumstreamwrite_put(desc->dsw[i], datum, isnull[i], &toFree1);
		if (toFree1 != NULL)
		{
			/*
			 * Use the de-toasted and/or de-compressed as datum instead.
			 */
			datum = PointerGetDatum(toFree1);
		}
		if (err < 0)
		{
			/*
			 * We have reached max number of datums that can be accommodated
			 * in current varblock.
			 */
			itemCount = datumstreamwrite_nth(desc->dsw[i]);
			/* write the block up to this one */
			datumstreamwrite_block(desc->dsw[i], &desc->blockDirectory, i + colno, true);
			if (itemCount > 0)
			{
				/* Next block's first row number */
				desc->dsw[i]->blockFirstRowNum += itemCount;
			}

			/* now write this new item to the new block */
			err = datumstreamwrite_put(desc->dsw[i], datum, isnull[i],
									   &toFree2);
			Assert(toFree2 == NULL);
			if (err < 0)
			{
				Assert(!isnull[i]);
				err = datumstreamwrite_lob(desc->dsw[i],
										   datum,
										   &desc->blockDirectory,
										   i + colno,
										   true);
				Assert(err >= 0);

				/*
				 * Have written the block above with column value
				 * corresponding to a row, so now update the first row number
				 * to correctly reflect for next block.
				 */
				desc->dsw[i]->blockFirstRowNum++;
			}
		}
		if (toFree1 != NULL)
			pfree(toFree1);
	}
}

void
aocs_addcol_finish(AOCSAddColumnDesc desc)
{
	int			i;

	aocs_addcol_closefiles(desc);
	AppendOnlyBlockDirectory_End_addCol(&desc->blockDirectory);
	for (i = 0; i < desc->num_newcols; ++i)
		destroy_datumstreamwrite(desc->dsw[i]);
	pfree(desc->dsw);
	desc->dsw = NULL;

	pfree(desc);
}

/*
 * Add empty VPEs (eof=0) to pg_aocsseg_* catalog, corresponding to
 * each new column being added.
 */
void
aocs_addcol_emptyvpe(Relation rel,
					 AOCSFileSegInfo **segInfos, int32 nseg,
					 int num_newcols)
{
	int			i;

	for (i = 0; i < nseg; ++i)
	{
		if (Gp_role == GP_ROLE_DISPATCH || segInfos[i]->total_tupcount == 0)
		{
			/*
			 * On QD, all tuples in pg_aocsseg_* catalog have eof=0. On QE,
			 * tuples with eof=0 may exist in pg_aocsseg_* already, caused by
			 * VACUUM.  We need to add corresponding tuples with eof=0 for
			 * each newly added column on QE.
			 */
			AOCSFileSegInfoAddVpe(rel, segInfos[i]->segno, NULL,
								  num_newcols, true /* empty VPEntry */ );
		}
	}
}


static int
aoco_acquire_sample_rows(Relation onerel, int elevel, HeapTuple *rows,
						 int targrows, double *totalrows, double *totaldeadrows)
{
	int		numrows = 0;	/* # rows now in reservoir */
	double	liverows = 0;	/* # live rows seen */
	double	deadrows = 0;	/* # dead rows seen */
	Snapshot	appendOnlyMetaDataSnapshot;

	Assert(targrows > 0);

	int			natts = RelationGetNumberOfAttributes(onerel);
	bool	   *proj = (bool *) palloc(natts * sizeof(bool));
	int			i;

	appendOnlyMetaDataSnapshot = GetTransactionSnapshot();

	for(i = 0; i < natts; i++)
		proj[i] = true;

	Assert(RelationIsAoCols(onerel));
									
    TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(onerel));
	AOCSScanDesc aocoscan =  aocs_beginscan(onerel,
									  SnapshotSelf,
									  appendOnlyMetaDataSnapshot,
									  RelationGetDescr(onerel), proj);

	int64 totaltupcount = AOCSScanDesc_TotalTupCount(aocoscan);
	int64 totaldeadtupcount = 0;
	if (aocoscan->total_seg > 0 )
		totaldeadtupcount = AppendOnlyVisimap_GetRelationHiddenTupleCount(&aocoscan->visibilityMap);
	/*
     * The conversion from int64 to double (53 significant bits) is safe as the
	 * AOTupleId is 48bits, the max value of totalrows is never greater than
	 * AOTupleId_MaxSegmentFileNum * AOTupleId_MaxRowNum (< 48 significant bits).
	 */
	*totalrows = (double) (totaltupcount - totaldeadtupcount);
	*totaldeadrows = (double) totaldeadtupcount;

	/* Prepare for sampling tuple numbers */
	RowSamplerData rs;
	RowSampler_Init(&rs, *totalrows, targrows, random());

	while (RowSampler_HasMore(&rs))
	{
		aocoscan->targrow = RowSampler_Next(&rs);

		vacuum_delay_point();

		if (aocs_get_target_tuple(aocoscan, aocoscan->targrow, slot))
		{
			rows[numrows++] = ExecCopySlotHeapTuple(slot);
			liverows++;
		}
		else
			deadrows++;
		
		ExecClearTuple(slot);
	}

	ExecDropSingleTupleTableSlot(slot);
	aocs_endscan(aocoscan);

	/*
	 * Emit some interesting relation info
	 */
	ereport(elevel,
			(errmsg("\"%s\": scanned " INT64_FORMAT " rows, "
					"containing %.0f live rows and %.0f dead rows; "
					"%d rows in sample, %.0f accurate total live rows, "
					"%.f accurate total dead rows",
					RelationGetRelationName(onerel),
					rs.m, liverows, deadrows, numrows,
					*totalrows, *totaldeadrows)));

	return numrows;
}