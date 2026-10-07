/*-------------------------------------------------------------------------
 *
 * custom_scan_test.c
 *		Test extension for CustomScan node infrastructure in Greenplum.
 *
 * When loaded (via LOAD or shared_preload_libraries), this extension
 * hooks into the planner and replaces sequential scans on heap tables
 * with a CustomScan node called "TestCustomScan". The custom scan
 * performs a simple sequential heap scan, same as SeqScan, but goes
 * through the full CustomScan path:
 *
 *   Planner hook -> CustomPath -> CustomScan plan -> CustomScanState
 *
 * This exercises:
 *   - set_rel_pathlist_hook
 *   - CustomPathMethods (PlanCustomPath)
 *   - CustomScanMethods (CreateCustomScanState)
 *   - CustomExecMethods (Begin/Exec/End/ReScan/Explain)
 *   - Plan serialization and dispatch to segments (GPDB)
 *
 * In addition, when a row exists in the metadata catalog
 * custom_scan_test.csv_files for the scanned relation, the custom scan
 * reads rows from the registered CSV file(s) instead of the underlying
 * heap. Each row of the catalog maps (relname, seg_id) to (csv_path,
 * ncols). seg_id = -1 means "any segment uses this path"; seg_id in
 * 0..N-1 pins a specific primary segment to a specific file.
 *
 * The CSV schema must match the scanned relation's tuple descriptor
 * (column count and input-function-parseable types). Empty fields are
 * treated as NULL. Each row of CSV becomes one tuple.
 *
 * Usage:
 *   LOAD 'custom_scan_test';
 *   SET custom_scan_test.enabled = on;
 *   EXPLAIN SELECT * FROM foo;
 *   -- Should show "Custom Scan (TestCustomScan)" instead of "Seq Scan"
 *
 *   -- CSV mode:
 *   CREATE TABLE employees (id int, name text, salary numeric);
 *   -- Tell the QD which file each segment should scan via a plain CSV
 *   -- metadata file: relname,seg_id,csv_path,ncols
 *   \! cat > /tmp/meta.csv <<EOF
 *   employees,0,/path/to/sample.csv,3
 *   employees,1,/path/to/sample.csv,3
 *   employees,2,/path/to/sample.csv,3
 *   EOF
 *   SET custom_scan_test.metadata_file = '/tmp/meta.csv';
 *   SET custom_scan_test.csv_has_header = on;
 *   SELECT * FROM employees;  -- data comes from the CSV, not the table
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbpathlocus.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/value.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

#define CSV_LINE_MAX 65536

/*
 * Locus mode selected by the user via the custom_scan_test.locus GUC.
 * Each value maps to one of the CdbPathLocus_Make* macros and controls
 * how the node is placed on segments by the planner, as well as whether
 * the CSV file is sliced across segments at execution time.
 */
typedef enum CustomScanLocusMode
{
	CSLM_INHERIT = 0,			/* cpath->path.locus = cheapest->locus */
	CSLM_ENTRY,					/* QD-only */
	CSLM_SEGMENT_GENERAL,		/* each segment has the same local copy */
	CSLM_GENERAL,				/* logically available everywhere */
	CSLM_STREWN,				/* each segment reads its own slice */
	CSLM_HASHED					/* hash-distributed on column 1 */
} CustomScanLocusMode;

static const struct config_enum_entry locus_mode_options[] = {
	{"inherit",         CSLM_INHERIT,         false},
	{"entry",           CSLM_ENTRY,           false},
	{"segment_general", CSLM_SEGMENT_GENERAL, false},
	{"general",         CSLM_GENERAL,         false},
	{"strewn",          CSLM_STREWN,          false},
	{"hashed",          CSLM_HASHED,          false},
	{NULL, 0, false}
};

/* GUC variables */
static bool		custom_scan_test_enabled = false;
static bool		custom_scan_test_csv_has_header = false;
static int		custom_scan_test_locus = CSLM_INHERIT;
static char	   *custom_scan_test_metadata_file = NULL;

/* Saved hook value */
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;

/* Forward declarations */
void		_PG_init(void);
void		_PG_fini(void);

static const char *
locus_mode_name(CustomScanLocusMode mode)
{
	switch (mode)
	{
		case CSLM_INHERIT:         return "inherit";
		case CSLM_ENTRY:           return "entry";
		case CSLM_SEGMENT_GENERAL: return "segment_general";
		case CSLM_GENERAL:         return "general";
		case CSLM_STREWN:          return "strewn";
		case CSLM_HASHED:          return "hashed";
	}
	return "unknown";
}

/*
 * Custom scan state: embeds CustomScanState as first field
 * so it can be cast freely.
 */
typedef struct TestCustomScanState
{
	CustomScanState css;

	/* Heap-scan mode */
	HeapScanDesc	heapScanDesc;

	/* CSV-scan mode */
	bool			useCsv;
	FILE		   *csvFile;
	char		   *csvFilePath;
	char		   *lineBuf;
	FmgrInfo	   *inputFuncs;
	Oid			   *typioparams;
	int				natts;

	/* Active locus mode (recovered from cscan->custom_private) */
	CustomScanLocusMode locusMode;

	/*
	 * CSV path and expected column count recovered from the plan's
	 * custom_private.  The QD resolved the full per-segment map during
	 * planning; this segment picks its own row here (matching seg_id to
	 * GpIdentity.segindex, falling back to the seg_id = -1 entry).
	 */
	char	   *planCsvPath;
	int			planNcols;

	/*
	 * CSV sharding across segments.  Active when locusMode is Strewn or
	 * Hashed: every segment reads only the rows whose (data-row-index
	 * mod numSegs) equals its own segindex.  dataRowNo counts data
	 * rows as they are read (header skipped, blank lines skipped).
	 */
	bool			sliced;
	int				segIdx;
	int				numSegs;
	int64			dataRowNo;
} TestCustomScanState;

/* Forward declarations used by the metadata reader. */
static bool read_csv_line(FILE *f, char *buf, size_t bufsize);
static int	parse_csv_line(char *line, char **fields, int maxfields);

/* ----------------------------------------------------------------
 * Metadata file reader
 * ----------------------------------------------------------------
 *
 * Metadata comes from a plain CSV file on the coordinator's filesystem,
 * pointed to by the custom_scan_test.metadata_file GUC.  Each data
 * line has four fields:
 *
 *     relname,seg_id,csv_path,ncols
 *
 * seg_id = -1 means "any segment"; seg_id >= 0 pins the row to a
 * specific primary segment.  The QD (planner hook) reads the file and
 * ships the parsed per-segment map to every segment inside the plan's
 * custom_private — the segments never touch this file themselves.
 *
 * out_entries is built as a List of 2-element Lists: each inner list
 * is [Integer(seg_id), String(csv_path)].  Fully serializable by
 * outfuncs/readfuncs, so it survives dispatch without extra work.
 */
static bool
read_metadata_file(const char *target_relname,
				   List **out_entries, int *out_ncols)
{
	FILE	   *f;
	char	   *linebuf;
	List	   *entries = NIL;
	int			ncols_seen = 0;

	*out_entries = NIL;
	*out_ncols = 0;

	if (custom_scan_test_metadata_file == NULL ||
		custom_scan_test_metadata_file[0] == '\0')
		return false;

	f = AllocateFile(custom_scan_test_metadata_file, "r");
	if (f == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open metadata file \"%s\": %m",
						custom_scan_test_metadata_file)));

	linebuf = palloc(CSV_LINE_MAX);

	while (read_csv_line(f, linebuf, CSV_LINE_MAX))
	{
		char	   *fields[4];
		int			nfields;
		int			seg_id;
		int			ncols_row;

		/* Skip blank and comment lines. */
		if (linebuf[0] == '\0' || linebuf[0] == '#')
			continue;

		nfields = parse_csv_line(linebuf, fields, 4);
		if (nfields != 4)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("bad metadata line in \"%s\" "
							"(expected 4 CSV fields, got %d)",
							custom_scan_test_metadata_file, nfields)));

		/* Only keep rows for the relation we are planning. */
		if (strcmp(fields[0], target_relname) != 0)
			continue;

		seg_id = atoi(fields[1]);
		ncols_row = atoi(fields[3]);

		/* Single expected ncols per relation. */
		if (ncols_seen == 0)
			ncols_seen = ncols_row;
		else if (ncols_seen != ncols_row)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("inconsistent ncols in metadata file for \"%s\" "
							"(saw %d and %d)",
							target_relname, ncols_seen, ncols_row)));

		entries = lappend(entries,
						  list_make2(makeInteger(seg_id),
									 makeString(pstrdup(fields[2]))));
	}

	FreeFile(f);
	pfree(linebuf);

	if (entries == NIL)
		return false;

	*out_entries = entries;
	*out_ncols = ncols_seen;
	return true;
}

/* ----------------------------------------------------------------
 * CSV helpers
 * ----------------------------------------------------------------
 */

/*
 * Read one line from the CSV file into buf (of size bufsize).
 * Trims trailing \r\n.
 * Returns true if a line was read, false on EOF.
 */
static bool
read_csv_line(FILE *f, char *buf, size_t bufsize)
{
	size_t		len;

	if (fgets(buf, bufsize, f) == NULL)
		return false;

	len = strlen(buf);

	if (len + 1 >= bufsize && buf[len - 1] != '\n' && !feof(f))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("CSV line exceeds maximum length of %d bytes",
						CSV_LINE_MAX - 1)));

	if (len > 0 && buf[len - 1] == '\n')
		buf[--len] = '\0';
	if (len > 0 && buf[len - 1] == '\r')
		buf[--len] = '\0';

	return true;
}

/*
 * Parse one CSV line.
 *
 * Returns the number of fields parsed.
 */
static int
parse_csv_line(char *line, char **fields, int maxfields)
{
	int			nfields = 0;
	char	   *p = line;

	/* An empty line produces a single empty field */
	if (*p == '\0')
	{
		fields[0] = p;
		return 1;
	}

	while (nfields < maxfields)
	{
		char	   *out;

		if (*p == '"')
		{
			/* Quoted field: unescape "" into " in place */
			p++;
			fields[nfields++] = p;
			out = p;
			while (*p != '\0')
			{
				if (*p == '"')
				{
					if (*(p + 1) == '"')
					{
						*out++ = '"';
						p += 2;
					}
					else
					{
						p++;	/* end of quoted field */
						break;
					}
				}
				else
					*out++ = *p++;
			}
			*out = '\0';
		}
		else
		{
			/* Plain field */
			fields[nfields++] = p;
			while (*p != '\0' && *p != ',')
				p++;
		}

		if (*p == ',')
		{
			*p = '\0';
			p++;
			/* trailing comma means one more empty field */
			if (*p == '\0')
			{
				if (nfields < maxfields)
					fields[nfields++] = p;
				break;
			}
		}
		else
			break;
	}

	return nfields;
}

/* ----------------------------------------------------------------
 * CustomExecMethods callbacks
 * ----------------------------------------------------------------
 */

static void
TestBeginCustomScan(CustomScanState *node, EState *estate, int eflags)
{
	TestCustomScanState *tss = (TestCustomScanState *) node;
	Relation	rel = node->ss.ss_currentRelation;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	const char *csv_to_open;

	/*
	 * Unpack the plan's custom_private. 
	 * Every segment sees exactly what the QD wrote in, because
	 * nodeToString/stringToNode round-trip the list verbatim.
	 */
	tss->planCsvPath = NULL;
	tss->planNcols = 0;
	tss->locusMode = CSLM_INHERIT;

	/*
	 * planSegExact = true means metadata gave us a file pinned to this
	 * specific segment, so no further in-file slicing is needed.
	 */
	bool		planSegExact = false;

	if (list_length(cscan->custom_private) >= 3)
	{
		List	   *entries;
		ListCell   *lc;
		const char *exact_path = NULL;
		const char *shared_path = NULL;
		int			my_segid = GpIdentity.segindex;

		tss->locusMode = (CustomScanLocusMode)
			intVal(linitial(cscan->custom_private));
		tss->planNcols = intVal(lsecond(cscan->custom_private));
		entries = (List *) lthird(cscan->custom_private);

		foreach(lc, entries)
		{
			List	   *entry = (List *) lfirst(lc);
			int			seg_id = intVal(linitial(entry));
			const char *path = strVal(lsecond(entry));

			if (seg_id == my_segid)
				exact_path = path;
			else if (seg_id == -1)
				shared_path = path;
		}

		if (exact_path != NULL)
		{
			tss->planCsvPath = pstrdup(exact_path);
			planSegExact = true;
		}
		else if (shared_path != NULL)
			tss->planCsvPath = pstrdup(shared_path);
	}

	/*
	 * Strewn and Hashed loci declare "each segment reads its own slice of
	 * the source.
	 */
	tss->sliced  = (tss->locusMode == CSLM_STREWN ||
					tss->locusMode == CSLM_HASHED) && !planSegExact;
	tss->segIdx  = GpIdentity.segindex;
	tss->numSegs = getgpsegmentCount();
	if (tss->numSegs <= 0)
		tss->numSegs = 1;
	tss->dataRowNo = 0;

	/*
	 * Decide whether to scan CSV or the heap.  The path is resolved
	 * entirely from the plan's custom_private, which the QD populated
	 * from the metadata catalog.  If no metadata row applied to this
	 * segment, planCsvPath is NULL and we fall through to heap mode.
	 */
	csv_to_open = tss->planCsvPath;

	if (csv_to_open != NULL)
	{
		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			natts = tupdesc->natts;
		int			i;

		/*
		 * If the metadata catalog told us how many columns to expect,
		 * verify the scanned relation agrees.
		 */
		if (tss->planNcols > 0 && tss->planNcols != natts)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("CSV metadata says %d column(s) but relation "
							"\"%s\" has %d",
							tss->planNcols,
							RelationGetRelationName(rel), natts)));

		tss->useCsv = true;
		tss->natts = natts;
		tss->csvFilePath = pstrdup(csv_to_open);

		tss->csvFile = AllocateFile(tss->csvFilePath, "r");
		if (tss->csvFile == NULL)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open CSV file \"%s\": %m",
							tss->csvFilePath)));

		tss->lineBuf = palloc(CSV_LINE_MAX);

		tss->inputFuncs = palloc(sizeof(FmgrInfo) * natts);
		tss->typioparams = palloc(sizeof(Oid) * natts);

		for (i = 0; i < natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);
			Oid			infuncid;

			getTypeInputInfo(att->atttypid, &infuncid, &tss->typioparams[i]);
			fmgr_info(infuncid, &tss->inputFuncs[i]);
		}

		/* Optionally skip a header row */
		if (custom_scan_test_csv_has_header)
			(void) read_csv_line(tss->csvFile, tss->lineBuf, CSV_LINE_MAX);
	}
	else
	{
		tss->useCsv = false;
		tss->heapScanDesc = heap_beginscan(rel,
										   estate->es_snapshot,
										   0,		/* nkeys */
										   NULL);	/* scan keys */
	}
}

/*
 * Fetch next tuple from the underlying heap relation.
 */
static TupleTableSlot *
TestHeapScanNext(CustomScanState *node)
{
	TestCustomScanState *tss = (TestCustomScanState *) node;
	EState	   *estate = node->ss.ps.state;
	ScanDirection direction = estate->es_direction;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	HeapTuple	tuple;

	tuple = heap_getnext(tss->heapScanDesc, direction);

	if (tuple)
		ExecStoreHeapTuple(tuple,
						   slot,
						   tss->heapScanDesc->rs_cbuf,
						   false);	/* don't pfree */
	else
		ExecClearTuple(slot);

	return slot;
}

/*
 * Fetch next tuple by reading one line from the configured CSV file.
 */
static TupleTableSlot *
TestCsvScanNext(CustomScanState *node)
{
	TestCustomScanState *tss = (TestCustomScanState *) node;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	Relation	rel = node->ss.ss_currentRelation;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	Datum	   *values;
	bool	   *isnull;
	char	  **fields;
	int			nfields;
	int			i;

	/*
	 * Read the next data row we are responsible for.  Blank lines are
	 * skipped.  In sliced mode every segment keeps only the rows whose
	 * (dataRowNo mod numSegs) matches its own segment index; the
	 * coordinator (segindex == MASTER_CONTENT_ID, i.e. -1) produces no
	 * rows in this mode.
	 */
	for (;;)
	{
		if (!read_csv_line(tss->csvFile, tss->lineBuf, CSV_LINE_MAX))
		{
			ExecClearTuple(slot);
			return slot;
		}
		if (tss->lineBuf[0] == '\0')
			continue;				/* skip blank lines */

		tss->dataRowNo++;

		if (tss->sliced)
		{
			if (tss->segIdx < 0)
				continue;			/* QD does not own any slice */
			if ((tss->dataRowNo - 1) % tss->numSegs != tss->segIdx)
				continue;			/* not our slice */
		}
		break;
	}

	fields = (char **) palloc(sizeof(char *) * tss->natts);
	nfields = parse_csv_line(tss->lineBuf, fields, tss->natts);

	if (nfields != tss->natts)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("CSV line has %d field(s), expected %d",
						nfields, tss->natts),
				 errdetail("File \"%s\": %s",
						   tss->csvFilePath, tss->lineBuf)));

	ExecClearTuple(slot);

	values = slot_get_values(slot);
	isnull = slot_get_isnull(slot);

	for (i = 0; i < tss->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		/* Empty unquoted field → NULL */
		if (fields[i][0] == '\0')
		{
			values[i] = (Datum) 0;
			isnull[i] = true;
		}
		else
		{
			values[i] = InputFunctionCall(&tss->inputFuncs[i],
										  fields[i],
										  tss->typioparams[i],
										  att->atttypmod);
			isnull[i] = false;
		}
	}

	pfree(fields);

	ExecStoreVirtualTuple(slot);
	return slot;
}

/*
 * Access method callback for ExecScan: fetch next tuple.
 */
static TupleTableSlot *
TestScanNext(CustomScanState *node)
{
	TestCustomScanState *tss = (TestCustomScanState *) node;

	if (tss->useCsv)
		return TestCsvScanNext(node);
	else
		return TestHeapScanNext(node);
}

static bool
TestScanRecheck(CustomScanState *node, TupleTableSlot *slot)
{
	/* No recheck needed */
	return true;
}

/*
 * Use ExecScan to get the standard qual-check and projection behavior.
 */
static TupleTableSlot *
TestExecCustomScan(CustomScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) TestScanNext,
					(ExecScanRecheckMtd) TestScanRecheck);
}

static void
TestEndCustomScan(CustomScanState *node)
{
	TestCustomScanState *tss = (TestCustomScanState *) node;

	if (tss->useCsv)
	{
		if (tss->csvFile)
		{
			FreeFile(tss->csvFile);
			tss->csvFile = NULL;
		}
	}
	else if (tss->heapScanDesc)
	{
		heap_endscan(tss->heapScanDesc);
		tss->heapScanDesc = NULL;
	}
}

static void
TestReScanCustomScan(CustomScanState *node)
{
	TestCustomScanState *tss = (TestCustomScanState *) node;

	if (tss->useCsv)
	{
		if (tss->csvFile)
		{
			rewind(tss->csvFile);
			tss->dataRowNo = 0;
			if (custom_scan_test_csv_has_header)
				(void) read_csv_line(tss->csvFile, tss->lineBuf, CSV_LINE_MAX);
		}
	}
	else if (tss->heapScanDesc)
		heap_rescan(tss->heapScanDesc, NULL);
}

static void
TestExplainCustomScan(CustomScanState *node, List *ancestors,
					  ExplainState *es)
{
	TestCustomScanState *tss = (TestCustomScanState *) node;

	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;

	ExplainPropertyText("Custom Scan Type", "TestCustomScan", es);
	ExplainPropertyText("Locus Mode", locus_mode_name(tss->locusMode), es);
	ExplainPropertyText("CSV Sliced Per Segment",
						tss->sliced ? "true" : "false", es);

	/* Show the full per-segment map the QD dispatched with the plan. */
	if (list_length(cscan->custom_private) >= 3)
	{
		List	   *entries = (List *) lthird(cscan->custom_private);

		ExplainPropertyInteger("CSV Metadata Entries",
							   list_length(entries), es);
		if (tss->planNcols > 0)
			ExplainPropertyInteger("CSV Expected Columns",
								   tss->planNcols, es);
	}

	if (tss->useCsv)
	{
		ExplainPropertyText("Source", "CSV file", es);
		ExplainPropertyText("CSV File",
							tss->csvFilePath ? tss->csvFilePath : "",
							es);
		ExplainPropertyText("CSV Has Header",
							custom_scan_test_csv_has_header ? "true" : "false",
							es);
	}
	else
	{
		ExplainPropertyText("Source", "heap", es);
	}
}

static const CustomExecMethods test_exec_methods = {
	.CustomName = "TestCustomScan",
	.BeginCustomScan = TestBeginCustomScan,
	.ExecCustomScan = TestExecCustomScan,
	.EndCustomScan = TestEndCustomScan,
	.ReScanCustomScan = TestReScanCustomScan,
	.MarkPosCustomScan = NULL,
	.RestrPosCustomScan = NULL,
	.ExplainCustomScan = TestExplainCustomScan,
};

/* ----------------------------------------------------------------
 * CustomScanMethods callback: create the execution state
 * ----------------------------------------------------------------
 */

static Node *
TestCreateCustomScanState(CustomScan *cscan)
{
	TestCustomScanState *tss;

	tss = (TestCustomScanState *) palloc0(sizeof(TestCustomScanState));
	NodeSetTag(tss, T_CustomScanState);
	tss->css.methods = &test_exec_methods;
	tss->heapScanDesc = NULL;
	tss->useCsv = false;
	tss->csvFile = NULL;

	return (Node *) tss;
}

static const CustomScanMethods test_scan_methods = {
	.CustomName = "TestCustomScan",
	.CreateCustomScanState = TestCreateCustomScanState,
};

/* ----------------------------------------------------------------
 * CustomPathMethods callback: convert Path to Plan
 * ----------------------------------------------------------------
 */

static Plan *
TestPlanCustomPath(PlannerInfo *root,
				   RelOptInfo *rel,
				   CustomPath *best_path,
				   List *tlist,
				   List *clauses,
				   List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	Index		scan_relid = rel->relid;
	List	   *scan_clauses;

	/* Extract plain qual expressions from RestrictInfo nodes */
	scan_clauses = extract_actual_clauses(clauses, false);

	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = scan_clauses;
	cscan->scan.plan.lefttree = NULL;
	cscan->scan.plan.righttree = NULL;
	cscan->scan.scanrelid = scan_relid;

	cscan->flags = best_path->flags;
	cscan->custom_plans = NIL;
	cscan->custom_exprs = NIL;
	cscan->custom_private = best_path->custom_private;
	cscan->custom_scan_tlist = NIL;
	cscan->custom_relids = NULL;
	cscan->methods = &test_scan_methods;

	return (Plan *) cscan;
}

static const CustomPathMethods test_path_methods = {
	.CustomName = "TestCustomScan",
	.PlanCustomPath = TestPlanCustomPath,
};

/* ----------------------------------------------------------------
 * Planner hook: inject CustomPath for plain heap relations
 * ----------------------------------------------------------------
 */

static void
set_path_locus(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte,
			   Path *cheapest, CustomPath *cpath)
{
	int		numsegments = cheapest->locus.numsegments > 0
						  ? cheapest->locus.numsegments
						  : getgpsegmentCount();

	switch ((CustomScanLocusMode) custom_scan_test_locus)
	{
		case CSLM_INHERIT:
			/* Safe default: mimic whatever distribution the relation has. */
			cpath->path.locus = cheapest->locus;
			break;

		case CSLM_ENTRY:
			/* Coordinator-only: the node runs only on the QD. */
			CdbPathLocus_MakeEntry(&cpath->path.locus);
			break;

		case CSLM_SEGMENT_GENERAL:
			/* Every segment has a local, identical copy of the source. */
			CdbPathLocus_MakeSegmentGeneral(&cpath->path.locus, numsegments);
			break;

		case CSLM_GENERAL:
			/* Source is logically available anywhere (pure/virtual). */
			CdbPathLocus_MakeGeneral(&cpath->path.locus, numsegments);
			break;

		case CSLM_STREWN:
			/* Each segment reads its own slice; no known partition key. */
			CdbPathLocus_MakeStrewn(&cpath->path.locus, numsegments);
			break;

		case CSLM_HASHED:
			{
				/*
				 * Build a DistributionKey on column 1 of the relation, using
				 * the type's default distribution opclass.  If the column
				 * type has no default distribution opclass (e.g. a custom
				 * type), fall back to inheriting — same safety net a real
				 * extension would want.
				 */
				Oid		typid;
				int32	typmod;
				Oid		collid;
				Oid		opclass;
				Oid		opfamily;
				Var	   *var;

				if (rel->max_attr < 1)
				{
					cpath->path.locus = cheapest->locus;
					break;
				}

				get_atttypetypmodcoll(rte->relid, 1,
									  &typid, &typmod, &collid);
				opclass = cdb_default_distribution_opclass_for_type(typid);
				if (!OidIsValid(opclass))
				{
					cpath->path.locus = cheapest->locus;
					break;
				}
				opfamily = get_opclass_family(opclass);
				var = makeVar(rel->relid, 1, typid, typmod, collid, 0);
				cpath->path.locus =
					cdbpathlocus_from_exprs(root,
											list_make1(var),
											list_make1_oid(opfamily),
											numsegments);
				break;
			}
	}
}

static void
test_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
					  Index rti, RangeTblEntry *rte)
{
	CustomPath *cpath;
	Path	   *cheapest;
	List	   *md_entries = NIL;
	int			md_ncols = 0;

	/* Call any previous hook first */
	if (prev_set_rel_pathlist_hook)
		prev_set_rel_pathlist_hook(root, rel, rti, rte);

	/* Only process if enabled */
	if (!custom_scan_test_enabled)
		return;

	/* Only plain heap relations */
	if (rel->rtekind != RTE_RELATION)
		return;
	if (rte->relkind != RELKIND_RELATION)
		return;
	if (rte->inh)
		return;

	/* We need at least one existing path to copy cost from */
	if (rel->pathlist == NIL)
		return;

	/*
	 * Read per-segment CSV routing from the metadata file on the QD.
	 * All matching rows are collected here and will be shipped to every
	 * segment via the plan's custom_private — the QD tells each QE
	 * exactly which file to scan.  If the file has no entry for this
	 * relation, md_entries stays NIL and the node falls back to heap
	 * scanning.
	 */
	{
		char	   *relname = get_rel_name(rte->relid);

		if (relname != NULL)
			(void) read_metadata_file(relname, &md_entries, &md_ncols);
	}

	cheapest = (Path *) linitial(rel->pathlist);

	/* Create a CustomPath */
	cpath = create_customscan_path(root, rel, NIL, 0, &test_path_methods);

	/* Copy cost estimates from the cheapest existing path, slightly cheaper */
	cpath->path.rows = cheapest->rows;
	cpath->path.startup_cost = cheapest->startup_cost;
	cpath->path.total_cost = cheapest->total_cost * 0.99;
	cpath->path.pathkeys = NIL;

	/* Demonstrate every locus constructor via the locus GUC. */
	set_path_locus(root, rel, rte, cheapest, cpath);

	/*
	 * Pack everything the executor and EXPLAIN will need into the path's
	 * custom_private.
	 */
	cpath->custom_private = list_make3(
		makeInteger((int) custom_scan_test_locus),
		makeInteger(md_ncols),
		md_entries);

	/* Add as an alternative path */
	add_path(rel, (Path *) cpath);
}

/* ----------------------------------------------------------------
 * Extension init/fini
 * ----------------------------------------------------------------
 */

void
_PG_init(void)
{
	/* Register the custom scan methods */
	RegisterCustomScanMethods(&test_scan_methods);

	/* Define GUCs */
	DefineCustomBoolVariable("custom_scan_test.enabled",
							 "Enable TestCustomScan for heap tables.",
							 NULL,
							 &custom_scan_test_enabled,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("custom_scan_test.metadata_file",
							   "Path to the CSV file holding per-relation, "
							   "per-segment scan routing metadata.",
							   "Each non-empty non-comment line is "
							   "\"relname,seg_id,csv_path,ncols\".  seg_id = -1 "
							   "means the path applies to any segment. The QD "
							   "reads this file at planning time and ships the "
							   "matching rows to every segment via the plan.",
							   &custom_scan_test_metadata_file,
							   "",
							   PGC_USERSET,
							   0,
							   NULL, NULL, NULL);

	/* Install the planner hook */
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = test_set_rel_pathlist;
}

void
_PG_fini(void)
{
	/* Uninstall the planner hook */
	set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
}
