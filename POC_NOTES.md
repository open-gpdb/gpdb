# POC: catalogless temp tables via CTAS (`gp_enable_catalogless_temp`)

Prototype of "catalogless" temporary tables for `CREATE TEMP TABLE ... AS
SELECT`. The goal of this branch is to estimate the code surface, not to
be functional or polished: correctness of runtime behavior was NOT
verified (the code compiles; it was not run).

## Architecture implemented

1. **GUC** `gp_enable_catalogless_temp` (bool, PGC_USERSET, default off) —
   `src/backend/utils/misc/guc_gp.c`, variable lives in
   `src/backend/cdb/cdbtempresult.c`.

2. **Session registry** — `src/backend/cdb/cdbtempresult.c`,
   `src/include/cdb/cdbtempresult.h`. Per-session hash
   `name -> {virtual_id, TupleDesc, GpPolicy, rowcount, NTupleStore}` in
   TopMemoryContext, present on QD and QEs. Virtual ids are a per-session
   counter assigned on the QD and dispatched inside the `IntoClause`.

3. **Write path** — `src/backend/commands/createas.c`:
   - `ExecCreateTableAs` marks the `IntoClause`
     (`isTempResult`/`tempResultId`, new fields serialized in
     out/read/copy/equal funcs) when the GUC is on and the target is
     `RELPERSISTENCE_TEMP`.
   - `intorel_initplan` short-circuits into `tempresult_initplan`: **no
     catalog rows at all** (no `DefineRelation`/`heap_create_with_catalog`,
     no toast/aoseg, no relfilenode). Registers the entry; on QEs creates
     a segment-local `NTupleStore` writer
     (`ntuplestore_create_readerwriter`) with deterministic name
     `CTMPRES_<gp_session_id>_<virtual_id>` (modeled on
     `shareinput_create_bufname_prefix`).
   - `intorel_receive`/`intorel_shutdown` append slots to the tuplestore /
     flush it; the store stays open until end of transaction.
   - QD records the global rowcount (`es_processed`) in its registry entry
     after `ExecutorEnd`.

4. **Read path** — `src/backend/parser/parse_relation.c`:
   `addRangeTableEntry` resolves an unqualified name against the registry
   *before* the catalog and builds a new RTE kind `RTE_TEMPRESULT`
   (`addRangeTableEntryForTempResult`). The RTE reuses the CTE fields
   (`ctename`, `ctecoltypes/typmods/collations`), which let `expandRTE`,
   `get_rte_attribute_type` etc. share the CTE code paths (pattern:
   minimal port of PG10 `RTE_NAMEDTUPLESTORE`, without QueryEnvironment).

5. **Planner** — new plan node `TempResultScan` + path:
   - `relnode.c`: `build_simple_rel` sets attrs and `rel->cdbpolicy` from
     the registry policy.
   - `allpaths.c`: `set_tempresult_size_estimates` (exact rowcount from
     registry) and `set_tempresult_pathlist`.
   - `pathnode.c`: `create_tempresultscan_path`; locus via
     `cdbpathlocus_from_baserel` over the registry `GpPolicy`, so a
     hash-distributed temp result joins on its distribution key without a
     Motion.
   - `costsize.c`: `cost_tempresultscan` (clone of `cost_valuesscan`).
   - `createplan.c`: `create_tempresultscan_plan`/`make_tempresultscan`;
     the plan node carries `tsname`, `tempresid` and the column type lists
     so any executor process can rebuild the scan TupleDesc without
     catalog/registry access.
   - `setrefs.c`, `subselect.c`, `cdbplan.c`, `cdbmutate.c`, `cdbllize.c`,
     `cdbpath.c`, `cdbtargeteddispatch.c`, `walkers.c`: plumbing cases.
   - **ORCA fallback**: `standard_planner` walks the query for
     `RTE_TEMPRESULT` and skips `optimize_query` (forced fallback to the
     Postgres planner) — the DXL translator has no representation for the
     new RTE kind.

6. **Executor** — `src/backend/executor/nodeTempResultScan.c` (+ header):
   opens the local ntuplestore lazily on first fetch (by name, reader
   mode) and returns tuples through `ExecScan`. Plumbed through
   `execProcnode.c`, `execAmi.c` (rescan seeks to BOF), `explain.c`
   ("Temp Result Scan").

7. **Serialization** — `outfuncs.c`/`outfast.c`, `readfuncs.c`/
   `readfast.c`, `copyfuncs.c`, `equalfuncs.c` for `TempResultScan`, the
   `IntoClause` fields and the `RTE_TEMPRESULT` case.

8. **Cleanup** — `RegisterXactCallback` in `cdbtempresult.c`: on top-level
   commit/abort all tuplestores (writer and tracked readers) are
   destroyed and the registry is emptied. Subtransactions: registering
   inside a subxact raises `ERROR` (POC scope).
   `DROP TABLE <name>` is intercepted at the top of `RemoveRelations`
   (`tablecmds.c`) and removes the entry locally.

## Stubbed / erroring out (ERRCODE_FEATURE_NOT_SUPPORTED)

- `WITH NO DATA` (`ExecCreateTableAs`)
- `ON COMMIT ...` clauses
- `CREATE TABLE AS EXECUTE`
- catalogless temp table inside a subtransaction (register time)
- `SELECT ... FOR UPDATE/SHARE` on a temp result (`analyze.c`)

## Not handled at all (name simply won't resolve / catalog error)

- INSERT/UPDATE/DELETE into the temp result (resolution goes through
  `setTargetTable` -> catalog -> "relation does not exist")
- indexes, ALTER, `\d` in psql, pg_dump, COPY, ANALYZE, VACUUM
- plain `CREATE TEMP TABLE` without `AS`

## Known holes / cheats

- **Reader gangs**: verified working in the smoke test (a self-join
  reads the store from a reader gang): the deterministic file name in
  the segment's shared `base/pgsql_tmp` directory makes the store
  findable from any process of the same segment, and the scan TupleDesc
  is rebuilt from the plan, so no registry access is needed for
  reading.  Still unhandled: a segment that received **zero** rows
  never creates the files, and a scan there would fail on open (did not
  occur with the test distributions).
- No synchronization between CTAS writer and later readers (the
  ShareInputScan-style ready/done FIFO protocol was not ported); safe
  only because statements are serialized within a session.
- The stores are opened with interXact = true and **no** workfile
  manager tracking (`ntuplestore_create_readerwriter_xact`), so
  gp_workfile_* views do not see them and per-query spill accounting
  does not apply.
- `DROP TABLE` on the QD removes only the QD entry; QE stores linger
  until end of transaction. Statement is not dispatched in that case.
- Volatile default: reading a temp result that was never written on a
  given segment (0 rows there) will still try to open the file — likely
  errors at runtime; needs "create empty store if missing" handling.
- Shadowing: a registry name shadows any catalog table with the same
  name for unqualified reads; no interaction rules were designed.
- `EXPLAIN (VERBOSE)`/ruleutils deparse only minimally handled.
- Memory-accounting reuses the ValuesScan owner tag for the new node.
- `TempResultScan` support was not added to mark/restore.

## Environment fixes (unrelated to the POC itself)

- `src/backend/cdb/motion/ic_udpifc.c`: current macOS SDKs no longer
  define `HZ` in `<sys/param.h>`; added `#ifndef HZ #define HZ 100`.
- `src/backend/gporca/gporca.mk`: homebrew xerces-c headers use C++11
  constructs; with `-std=gnu++98 -Werror -Wpedantic` the ORCA build dies.
  Added `-Wno-c++11-extensions -Wno-long-long`.

## Estimate of remaining work to production

- Correct cross-gang/cross-slice visibility of the stores (reader-gang
  problem above); likely needs writer/reader synchronization a la
  ShareInputScan and "missing file = empty" semantics: **large**.
- DML (INSERT/UPDATE/DELETE), or at least clean errors for it.
- ORCA support (new DXL operator or a proper fallback annotation).
- Spill/memory accounting, workfile manager integration, statement_mem.
- Interaction with catalog names, search_path semantics, EXPLAIN,
  ruleutils, views over temp results, pg_temp handling.
- Subtransactions/savepoints, ON COMMIT semantics, 2PC interaction of
  the xact callback.
- Tests (regress + isolation), \d support, pg_dump behavior definition.

Rough estimate: the POC is ~10-15% of a production implementation.

## Live smoke test (2026-08-31)

Ran on a 2-segment gpdemo cluster built from this branch (macOS arm64):
`test/catalogless_smoke.sql`, full output in `test/catalogless_smoke.out`.

Verified:

1. **Zero catalog rows** while the catalogless temp table is alive:
   `pg_class`/`pg_attribute`/`pg_type`/`pg_depend` counts are byte-for-byte
   identical to the baseline on the QD and on both segments
   (438/3376/441/8072), and `pg_class` has no `poc_t` row anywhere.
   Control CTAS with the GUC off adds +1/+9/+2/+3 rows on QD and each
   segment.
2. **Reads work and plan correctly**: `count/sum` over 1000 rows correct
   (1000/1001000); WHERE + ORDER BY correct; join with a co-distributed
   heap table shows `Temp Result Scan` with a co-located Hash Join and
   **no Redistribute Motion** (hashed locus from the recorded policy,
   rows=500/segment from the exact rowcount); self-join re-reads the
   tuplestore (reader gang) correctly.
3. **Transaction scope**: after COMMIT the name no longer resolves and the
   `pgsql_tmp_CTMPRES_*` files are removed from both segments (verified
   present during the transaction, absent after).
4. **Negatives**: INSERT and CREATE INDEX on a catalogless temp table fail
   with a clean `relation ... does not exist` error (the name is invisible
   to the catalog-based resolution of DML/DDL) — no crash.  DROP TABLE
   removes the entry.

Bugs found & fixed during bring-up (separate commits):

- GUC missing from `sync_guc_name.h` → server FATALs at startup.
- Tuplestore files were opened with interXact = false → closed/deleted
  by the resource owner at the end of the creating statement; added
  `ntuplestore_create_readerwriter_xact` (interXact = true, registry
  owns the lifecycle).
- `find_indexkey_var` fell back to a pg_attribute lookup with relid 0
  for a distribution key column absent from reltargetlist
  (`SELECT count(*)`) → take type info from the RTE.

## How to start the demo cluster

The worktree has its own install prefix (never touches the user's
GPHOME): configure with `--prefix=/Users/alena/open-gpdb3-poc/idea1/install`
(all other options as in the main tree) and:

```sh
cd /Users/alena/open-gpdb3-poc/idea1
make -j8 -C src/backend install
make -C gpMgmt/bin psutil pyyaml CC="gcc -Wno-error=implicit-function-declaration"  # psutil 5.7.0 vs new clang
make -C gpMgmt install
make -C gpcontrib/gp_internal_tools install    # gp_resource_group.so needed by initdb

export GPHOME=/Users/alena/open-gpdb3-poc/idea1/install
source $GPHOME/greenplum_path.sh
export MASTER_DATA_DIRECTORY=/Users/alena/open-gpdb3-poc/idea1/gpAux/gpdemo/datadirs/qddir/demoDataDir-1

# first-time init (non-standard ports to avoid the user's clusters):
cd gpAux/gpdemo
export DEMO_PORT_BASE=16432 NUM_PRIMARY_MIRROR_PAIRS=2 WITH_MIRRORS=false \
       DATADIRS=/Users/alena/open-gpdb3-poc/idea1/gpAux/gpdemo/datadirs
bash demo_cluster.sh        # gpinitsystem's own final gpstart may fail; then:
pg_ctl -D $MASTER_DATA_DIRECTORY stop -m fast   # stop the utility-mode master it left
gpstart -a

# regular start/stop afterwards:
gpstart -a
gpstop -a

# run the smoke test:
createdb -p 16432 pocdb
DATADIRS=/Users/alena/open-gpdb3-poc/idea1/gpAux/gpdemo/datadirs \
  psql -p 16432 pocdb -e -f test/catalogless_smoke.sql
```

Cluster: master :16432, primaries :16434/:16435, data under
`gpAux/gpdemo/datadirs` inside the worktree.  Build-artifact directories
`install/`, `gpAux/gpdemo/datadirs/` and the extracted
`gpMgmt/bin/pythonSrc/ext/*` are deliberately left untracked.

Environment quirks hit during bring-up (documented, not POC bugs): the
top-level `make install` fails in `contrib/hstore` on this SDK (not
needed); `gpstart` needs python2 `psutil`, whose 5.7.0 sources need
`-Wno-error=implicit-function-declaration` with current clang.

## Diffstat

(vs OPENGPDB_STABLE; `git diff OPENGPDB_STABLE --stat | tail -5`)

```
 src/include/utils/tuplestorenew.h         |   1 +
 test/catalogless_smoke.out                | 274 ++++++++++++++++++++++++++++++
 test/catalogless_smoke.sql                | 160 +++++++++++++++++
 57 files changed, 2168 insertions(+), 8 deletions(-)
```

Build: configured with the same options as the main tree
(`--with-perl --without-openssl --without-gssapi --with-libxml
--without-mdblocales --without-zstd --without-python --without-icu
CFLAGS/CXXFLAGS='-O0 -g3'`); `make -j8 -C src/backend` completes and
links the `postgres` binary (ORCA included, with the two environment
fixes above).
