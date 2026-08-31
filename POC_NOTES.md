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

- **Reader gangs**: the tuplestore is written by the QE *writer* gang.
  A later scan dispatched to a reader gang process will not find the
  file (`CTMPRES_...`) since workfiles are per-process-visible via the
  shared temp dir — the deterministic name makes them findable
  segment-wide, but registry entries only exist in the writer processes.
  The scan TupleDesc is rebuilt from the plan, so the scan itself does
  not need the registry, but `ntuplestore_create_readerwriter` in reader
  mode expects the file to exist and be complete.
- No synchronization between CTAS writer and later readers (the
  ShareInputScan-style ready/done FIFO protocol was not ported); safe
  only because statements are serialized within a session.
- Workfile-manager lifetime: NTupleStore workfiles are normally scoped
  to a query; keeping the store open across statements (until xact end)
  in TopMemoryContext is not something workfile_mgr accounting expects.
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

## Diffstat

(vs OPENGPDB_STABLE; `git diff OPENGPDB_STABLE --stat | tail -5`)

```
 src/include/nodes/primnodes.h             |   4 +
 src/include/optimizer/cost.h              |   2 +
 src/include/optimizer/pathnode.h          |   2 +
 src/include/parser/parse_relation.h       |   7 +
 50 files changed, 1409 insertions(+), 8 deletions(-)
```

Build: configured with the same options as the main tree
(`--with-perl --without-openssl --without-gssapi --with-libxml
--without-mdblocales --without-zstd --without-python --without-icu
CFLAGS/CXXFLAGS='-O0 -g3'`); `make -j8 -C src/backend` completes and
links the `postgres` binary (ORCA included, with the two environment
fixes above).
