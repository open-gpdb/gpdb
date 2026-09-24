# POC: catalogless temporary tables via CTAS (`gp_enable_catalogless_temp`)

A prototype of "catalogless" temporary tables for `CREATE TEMP TABLE ... AS
SELECT`. The goal of the branch is to estimate the amount of code, not to
deliver a finished feature. At first the code only compiled; later the
prototype was brought up on a live demo cluster and checked with smoke and
load tests (see the test sections below). On 2026-09-24 the issues found in
review were fixed and the history was rewritten layer by layer (see "Review
fixes" and "History layout").

## Architecture

1. **Option parsing** (parse analysis): a WITH option of the CTAS itself —

   ```sql
   CREATE TEMP TABLE t WITH (catalogless[=true|false]) AS SELECT ...
   ```

   The option is recognized in `transformCataloglessOption`
   (`parser/analyze.c`), called from `transformCreateTableAsStmt`: a **new**
   `into->options` list is built without it, and its value is stored in
   `IntoClause.isTempResult`. Parse analysis always works on a fresh copy of
   the raw tree (plancache copies `raw_parse_tree` before analysis), so a
   CTAS from a cached plpgsql plan and `EXPLAIN` see exactly what a plain
   CTAS sees. Syntactic restrictions are checked there as well: non-TEMP,
   matview, a schema other than `pg_temp`, `WITH NO DATA`, `ON COMMIT`,
   `AS EXECUTE`, a repeated option. `EXPLAIN ANALYZE` of such a CTAS is a
   "not implemented" error (plain `EXPLAIN` shows the plan).

   At execution time `ExecCreateTableAs` calls `TempResultPrepareInto`: it
   checks the kill-switch and name conflicts and returns a **copy** of the
   `IntoClause` with a new virtual id — the statement itself may be a cached
   plan and is never modified.

   **GUC** `gp_enable_catalogless_temp` (bool, **PGC_SUSET**, default on) is
   a kill-switch for *creation* only. Name lookup does not depend on it, so
   turning the GUC off in the middle of a transaction does not change what a
   name resolves to.

2. **Session registry** — `src/backend/cdb/cdbtempresult.c`,
   `src/include/cdb/cdbtempresult.h`. A per-session hash
   `name -> {vid, TupleDesc, GpPolicy, rowcount, NTupleStore}` in
   TopMemoryContext, present on the QD and on the QE writers.
   - **Identity is the vid** (a per-session counter on the QD, `int32` with
     an overflow check): it travels in the `IntoClause`, in the RTE
     (`RangeTblEntry.tempresid`) and in the plan node. The name is only used
     for resolution; the planner looks entries up with
     `TempResultLookupId(name, vid)` and fails with a clear error if the
     table was recreated.
   - **Name resolution** (`TempResultResolve`): the table behaves as if it
     lived in `pg_temp`. `pg_temp.t` always finds it; an unqualified `t`
     only if `pg_temp` comes before the other schemas for that name in
     `search_path` (`RelnameTempNamespaceFirst` in `namespace.c` mirrors
     `recomputeNamespacePath`: an implicit `pg_temp` goes first, an explicit
     one sits at its position after an implicit `pg_catalog`). Any other
     schema never finds it.
   - **Catalog paths are closed explicitly**: `RangeVarGetRelidExtended`
     raises `ERROR: "t" is a catalogless temp table` for a name that
     resolves to a catalogless table, so INSERT/UPDATE/DELETE/TRUNCATE/
     ALTER/COPY/LOCK/ANALYZE/CREATE INDEX no longer silently hit a shadowed
     catalog table. `heap_create_with_catalog` refuses to create a regular
     temp relation with the name of a catalogless table, and vice versa.
   - A limit of `TEMPRESULT_MAX_ENTRIES` (1024) live tables; an entry is
     inserted into the hash only after all allocations succeeded.
   - Creation and DROP inside a subtransaction are refused (the registry is
     not rolled back by `ROLLBACK TO SAVEPOINT`).
   - Registration, DROP and end-of-transaction cleanup call
     `ResetPlanCache()` on the QD: the tables have no OIDs and hence no
     relcache invalidation, so cached plans (plpgsql, prepared) get
     re-analyzed.
   - `DROP TABLE` is intercepted in `RemoveRelations` on a **copy** of the
     `DropStmt`; `ExecDropStmt` dispatches the original statement, so the QE
     writers drop their entry and files through the same code.

3. **Files: write and read** — `createas.c`, `tuplestorenew.c`,
   `cdbtempresult.c`:
   - `intorel_initplan` branches into `tempresult_initplan`: **not a single
     catalog row** (no `DefineRelation`/`heap_create_with_catalog`, no
     toast/aoseg, no relfilenode). It registers the entry; on a QE it creates
     a segment-local writer `NTupleStore`
     (`ntuplestore_create_readerwriter_xact`: interXact, no workfile
     manager) with the deterministic name `CTMPRES_<gp_session_id>_<vid>`
     (after `shareinput_create_bufname_prefix`). The store is created on
     every segment of the writer gang, even if no rows go there.
   - `intorel_receive`/`intorel_shutdown` append slots to the tuplestore /
     flush it; the store lives until the end of the transaction. After
     `ExecutorEnd` the QD records the total row count in its entry.
   - A reader (`TempResultOpenReader`) opens the store by name and is closed
     in the scan's `ExecEnd` (`TempResultCloseReader`); the list of open
     readers only matters on error paths.
   - Cleanup is an xact callback on **PRE_COMMIT / PRE_PREPARE / ABORT**
     (not on COMMIT: an error after the commit record is written is a PANIC;
     PRE_PREPARE covers the QEs in 2PC).
   - After a segment crash-restart the postmaster calls
     `RemovePgTempFiles()` (like `remove_temp_files_after_crash` in PG14): a
     crashed QE never gets to delete its interXact files.

4. **Planner** — `RTE_TEMPRESULT` and the `TempResultScan` plan node:
   - `parse_relation.c`: on the QD `addRangeTableEntry` resolves the name
     through `TempResultResolve` and builds an `RTE_TEMPRESULT`
     (`addRangeTableEntryForTempResult`). The RTE reuses the CTE fields
     (`ctename`, `ctecoltypes/typmods/collations`) for the name and columns —
     so `expandRTE`, `get_rte_attribute_type` etc. go through the CTE
     branches (the `RTE_NAMEDTUPLESTORE` pattern from PG10) — and carries its
     own `tempresid` field.
   - Views, rules and matviews over catalogless tables are refused
     (`DefineQueryRewrite`), and the text `readfuncs.c` (the catalog format)
     refuses to read `RTE_TEMPRESULT` — a reference to such a table cannot
     end up in `pg_rewrite`.
   - `relnode.c`: `build_simple_rel` takes the attributes and
     `rel->cdbpolicy` from the registry entry (no policy is an error, not a
     silent Entry locus).
   - `allpaths.c`: `set_tempresult_size_estimates` (exact row count) and
     `set_tempresult_pathlist`; `pathnode.c`: `create_tempresultscan_path`
     with the locus from the `GpPolicy` (hash — co-located join without a
     Motion, randomly — Strewn, replicated — SegmentGeneral); `costsize.c`:
     `cost_tempresultscan`; `createplan.c`: `make_tempresultscan` — the node
     carries `tsname`, `tempresid` and the column types, so any process can
     rebuild the TupleDesc without the catalog or the registry.
   - `cdbtargeteddispatch.c`: `TempResultScan` disables direct dispatch
     (previously it was ignored like `ValuesScan`, although the data is
     partitioned).
   - **ORCA is not called** for queries with `RTE_TEMPRESULT`: the rule
     lives in `isQueryForOrca()` next to upstream's other ORCA bypass rules
     (`isQueryUsingTempResult`; the check is cheap: only while the registry
     is not empty) — the DXL translator has no such RTE kind.
   - Plumbing: `setrefs.c`, `subselect.c`, `cdbplan.c`, `cdbmutate.c`,
     `cdbllize.c`, `cdbpath.c`, `walkers.c`, `nodeFuncs.c`, `ruleutils.c`,
     serialization of `TempResultScan`/`RTE_TEMPRESULT` in
     out/read/outfast/readfast/copy/equal.

5. **Executor** — `src/backend/executor/nodeTempResultScan.c` (+ header):
   opens the local ntuplestore lazily on the first fetch (by name, reader
   mode), returns tuples through `ExecScan`, closes the reader in `ExecEnd`.
   Plumbing in `execProcnode.c`, `execAmi.c` (rescan — seek to BOF),
   `explain.c` ("Temp Result Scan").

## Stubbed / raises an error (ERRCODE_FEATURE_NOT_SUPPORTED)

- `WITH NO DATA`, `ON COMMIT ...`, `CREATE TABLE AS EXECUTE`
- `EXPLAIN ANALYZE CREATE TEMP TABLE ... WITH (catalogless) AS ...`
- creation or DROP inside a subtransaction
- `SELECT ... FOR UPDATE/SHARE` on a temp result (`analyze.c`)
- any catalog operation by name: INSERT/UPDATE/DELETE, TRUNCATE, ALTER,
  CREATE INDEX, COPY t, LOCK, ANALYZE (`RangeVarGetRelidExtended`)
- views, rules, matviews over a temp result

## Not handled

- `\d` in psql, pg_dump — the table is not in the catalog.
- A plain `CREATE TEMP TABLE` without `AS`.
- Bodies of SQL/plpgsql functions executed on segments (ORCA may place a
  function on a QE): QEs do not resolve names through the registry.

## Known holes / shortcuts

- No synchronization between the CTAS writer and later readers (the
  ShareInputScan ready/done FIFO protocol is not ported); safe only because
  statements within a session are serialized.
- Stores are not accounted by the workfile manager: `gp_workfile_*` limits
  and views do not see them; only `temp_file_limit` (per process) applies.
  interXact forces the files into the default tablespace, `temp_tablespaces`
  is ignored.
- The lifetime is one transaction: in autocommit the table disappears right
  after the CTAS, and an ETL job has to keep a transaction open, holding back
  the cluster's xmin horizon. A session-level lifetime would require keeping
  the state somewhere other than the QE writer process (which
  `gp_vmem_idle_resource_timeout` kills).
- No statistics (only the exact row count): default selectivities.
- `ResetPlanCache()` on every creation/DROP drops all cached plans of the
  session — correct, but coarse.
- `EXPLAIN (VERBOSE)`/deparse in ruleutils is handled minimally
  (`add_rte_to_flat_rtable` clears `ctecol*` in the final rtable).
- Memory accounting reuses the ValuesScan owner tag for the new node.
- No mark/restore support for `TempResultScan`.
- An override search path (`PushOverrideSearchPath`) is handled
  conservatively: the table is visible only if the path has no relation of
  the same name.

## Environment fixes (unrelated to the POC itself)

These are **not part of this branch**, so that it merges into
OPENGPDB_STABLE cleanly. They live in the local branch
`build/macos-sdk-fix` (one commit); cherry-pick them without committing
(`git cherry-pick -n build/macos-sdk-fix`) to build on a recent macOS:

- `src/backend/cdb/motion/ic_udpifc.c`: recent macOS SDKs no longer define
  `HZ` in `<sys/param.h>`; added `#ifndef HZ #define HZ 100`.
- `src/backend/gporca/gporca.mk`: homebrew xerces-c headers use C++11
  constructs; with `-std=gnu++98 -Werror -Wpedantic` the ORCA build fails.
  Added `-Wno-c++11-extensions -Wno-long-long`.

## Remaining work before production

- ShareInputScan-style writer/reader synchronization for cross-slice reads
  within one statement.
- Session-level lifetime (see above) and `ON COMMIT` semantics.
- DML (INSERT/UPDATE/DELETE) into a temp result.
- ORCA support (a new DXL operator or an honest fallback annotation).
- Spill/memory accounting, workfile manager integration, statement_mem.
- Subtransactions/savepoints (a registry that is transactional per subxact
  id).
- Statistics collected while writing (ndistinct/min/max) instead of default
  selectivities.
- Tests (regress + isolation), `\d` support, defined pg_dump behavior.

Rough estimate: the POC is ~10–15% of a production implementation.

## Live smoke test (2026-08-31)

Run on a 2-segment gpdemo cluster built from this branch (macOS arm64):
`test/catalogless_smoke.sql`, full output in `test/catalogless_smoke.out`.

Verified:

1. **Zero catalog rows** while the catalogless temp table is alive: the
   `pg_class`/`pg_attribute`/`pg_type`/`pg_depend` counters match the
   baseline exactly on the QD and both segments (438/3376/441/8072), and
   there is no `poc_t` row in `pg_class` anywhere. The control CTAS without
   the catalogless path (now checked as `WITH (catalogless=false)`, which
   must be consumed rather than reach reloption validation) adds
   +1/+9/+2/+3 rows on the QD and each segment and does create a `pg_class`
   row.
2. **Reads work and are planned correctly**: `count/sum` over 1000 rows are
   correct (1000/1001000); WHERE + ORDER BY are correct; a join with a
   co-distributed heap table shows `Temp Result Scan` with a co-located Hash
   Join and **no Redistribute Motion** (hashed locus from the stored policy,
   rows=500/segment from the exact row count); a self-join re-reads the
   tuplestore (from a reader gang) correctly.
3. **Transactional scope**: after COMMIT the name no longer resolves, and
   the `pgsql_tmp_CTMPRES_*` files are removed from both segments (checked:
   present during the transaction, gone after).
4. **Negative cases**: INSERT and CREATE INDEX on a catalogless temp table
   fail with a clean error — no crashes (originally `relation ... does not
   exist`, since 2026-09-24 `"poc_neg1" is a catalogless temp table`). DROP
   TABLE removes the entry. Two more cases for the per-object syntax:
   `CREATE TABLE ... WITH (catalogless=true)` (without TEMP) →
   `ERROR: catalogless requires TEMP`; the option with
   `gp_enable_catalogless_temp = off` → `ERROR: catalogless temp tables are
   disabled` (both recorded in `test/catalogless_smoke.out`).

Bugs found and fixed while bringing it up:

- The GUC was missing from `sync_guc_name.h` → server FATAL at startup.
- Tuplestore files were opened with interXact = false → closed and deleted
  by the resource owner at the end of the creating statement; added
  `ntuplestore_create_readerwriter_xact` (interXact = true, lifecycle owned
  by the registry).
- `find_indexkey_var` fell back to a pg_attribute lookup with relid 0 for a
  distribution key column missing from the reltargetlist (`SELECT
  count(*)`) → the types are now taken from the RTE.

## Load test (2026-08-31)

`test/catalogless_bigdata.sql` (output: `test/catalogless_bigdata.out`), the
same 2-segment demo cluster. A readerwriter NTupleStore is always file-backed
and, with maxBytes = 0 as the POC passes, keeps an in-memory window of only
16 pages of 32KB = **512KB per store**; the test drives eviction/reload far
beyond it. All phases passed, nothing had to be fixed:

1. **20M narrow rows** (`id int, v bigint`): CTAS 14.3s, spill file
   `pgsql_tmp_CTMPRES_*` = **268MB per segment**. `count/sum` are exact
   (20000000 / 1400000070000000 = 7·n·(n+1)/2), a sampled `WHERE id IN` is
   exact, a repeated full `count(*)` (a pure re-read from disk) takes 1.5s.
2. **Writer QE RSS** (sampled every 4s): 84MB before, **119MB flat during
   the whole 268MB write**, 119.6MB after the reads — memory does not grow
   with the data volume; there is only the 512KB window plus motion/executor
   overhead. (The late 224MB peak belongs to the join hash table — 500K heap
   rows per segment — not to the tuplestore.)
3. **LOB path**: with BLCKSZ=32768, NTS_MAX_ENTRY_SIZE ≈ 32700 bytes, so the
   proposed 2KB pad would have stayed inline; `repeat('x',40000)` was used
   instead (40KB tuples, 20k rows). The `_LOB` file = **381–383MB per
   segment**, `count`/`sum(length)`/sampled lengths are exact. The LOB write
   and read paths work unchanged.
4. **Join at volume** (20M temp result × 1M co-distributed heap, count =
   1000000 in both variants):
   - catalogless (Temp Result Scan, Postgres planner due to the forced
     fallback): EXPLAIN ANALYZE 4.9s, scan of 10M rows/segment 1.49s;
   - control heap temp table (ORCA chose the plan): EXPLAIN ANALYZE 5.3s,
     Seq Scan 1.8s; its CTAS took 16.5s vs 14.3s.
   I.e. reading 20M rows from an ntuplestore is on par with a heap scan of
   the same data (even slightly faster here); -O0 build, single host —
   treat it as a smoke-level comparison.
5. **COMMIT**: `pgsql_tmp` is empty on both segments, `find *CTMPRES*` = 0.

A note on the 512KB window: it only limits each store's page cache (write
speed / locality of re-reads), not correctness. Going forward the window
should be sized like other operators' — from `statement_mem`/operator memory
(the write side already gets `PlanStateOperatorMemKB`, but the reader in the
POC passes maxBytes = 0) or a dedicated GUC; a larger window mostly helps
repeated small range scans, sequential full scans are fine as is.

On 2026-09-24 the test was rerun on the rebuilt branch: the result rows are
identical.

## Bringing up the demo cluster

The worktree has its own install prefix (the user's GPHOME is untouched):
configure with `--prefix=/Users/alena/open-gpdb3-poc/idea1/install` (other
options as in the main tree) and:

```sh
cd /Users/alena/open-gpdb3-poc/idea1
make -j8 -C src/backend install
make -C gpMgmt/bin psutil pyyaml CC="gcc -Wno-error=implicit-function-declaration"  # psutil 5.7.0 vs recent clang
make -C gpMgmt install
make -C gpcontrib/gp_internal_tools install    # initdb needs gp_resource_group.so

export GPHOME=/Users/alena/open-gpdb3-poc/idea1/install
source $GPHOME/greenplum_path.sh
export MASTER_DATA_DIRECTORY=/Users/alena/open-gpdb3-poc/idea1/gpAux/gpdemo/datadirs/qddir/demoDataDir-1

# initial setup (non-default ports, so as not to touch the user's clusters):
cd gpAux/gpdemo
export DEMO_PORT_BASE=17432 NUM_PRIMARY_MIRROR_PAIRS=2 WITH_MIRRORS=false \
       DATADIRS=/Users/alena/open-gpdb3-poc/idea1/gpAux/gpdemo/datadirs
bash demo_cluster.sh        # the final gpstart inside gpinitsystem may fail; then:
pg_ctl -D $MASTER_DATA_DIRECTORY stop -m fast   # stop the utility-mode master it left behind
gpstart -a

# regular start/stop afterwards:
gpstart -a
gpstop -a

# running the smoke test:
createdb -p 17432 pocdb
DATADIRS=/Users/alena/open-gpdb3-poc/idea1/gpAux/gpdemo/datadirs \
  psql -p 17432 pocdb -e -f test/catalogless_smoke.sql
```

Cluster: master :17432, primaries :17434/:17435, data in
`gpAux/gpdemo/datadirs` inside the worktree. The build directories
`install/`, `gpAux/gpdemo/datadirs/` and the unpacked
`gpMgmt/bin/pythonSrc/ext/*` are intentionally left untracked.

Note that the build has no automatic header dependency tracking
(`autodepend` is empty): after changing a header, delete the backend objects
and the `objfiles.txt` stamps (outside `src/backend/gporca`) and rebuild.

Environment quirks met along the way (documented, not POC bugs): the
top-level `make install` fails in `contrib/hstore` on this SDK (not needed);
`gpstart` needs python2 `psutil`, whose 5.7.0 sources need
`-Wno-error=implicit-function-declaration` with the current clang.

## Plan test (2026-09-13)

`test/catalogless_plans.sql` (output: `test/catalogless_plans.out`): six
query shapes over catalogless tables, each with EXPLAIN and a bit-for-bit
count/sum comparison against ordinary temp twins holding the same data; for
every created table, the pg_class/pg_attribute/pg_type/pg_depend delta on the
QD and both segments (summary table in the output: catalogless — strictly 0
everywhere, ordinary temp — +1/+10/+2/+3 per node).

1. join on the distribution key — Temp Result Scan without Redistribute;
2. join on a non-key — the planner adds a Broadcast Motion (the sides are
   not falsely treated as co-located);
3. GROUP BY a non-key — two-phase HashAggregate with a Redistribute over the
   Temp Result Scan;
4. join of two catalogless tables with different keys — the side with the
   matching key stays in place, the other one is redistributed;
5. EXISTS (semi-join) — works after a fix: `pathnode_walk_kids` (cdbpath
   dedup) did not know T_TempResultScan and failed with "unrecognized path
   type: 126"; it was added to the leaf path list;
6. ORDER BY + LIMIT — Sort+Limit on the segments, Gather, final Limit.

All six results matched the controls bit for bit.

## Review fixes (2026-09-24)

`test/catalogless_fixes.sql` (output: `test/catalogless_fixes.out`), the same
2-segment demo cluster. Each block checks one defect found in review; all
give the expected result:

1. **Mutation of a cached CTAS**: the option used to be removed from
   `into->options` in `ExecCreateTableAs`, and a second CTAS call from a
   plpgsql function ran with the "old" `isTempResult`/vid, bypassing the
   checks. Now parsing happens in parse analysis and the vid goes on a copy.
   Two calls in different transactions are both catalogless (0 `pg_class`
   rows), the kill-switch fires on the third call; `SET` of the kill-switch
   by a regular user gives `permission denied`.
2. `EXPLAIN` of a catalogless CTAS shows the plan; `EXPLAIN ANALYZE` is a
   clean error (the option used not to be consumed on that path);
   `WITH (catalogless, catalogless=false)` gives `conflicting or redundant
   options`.
3. **search_path**: with `public, pg_temp` the catalog table
   `public.fx_shadow` is read, with an implicit or leading `pg_temp` the
   catalogless one, and `pg_temp.fx_shadow` is always the catalogless one.
   The registry used to win always, including over `pg_catalog` and in
   SECURITY DEFINER functions with a "secure" search_path.
4. `INSERT`/`TRUNCATE` by the name of a catalogless table while a catalog
   table of the same name exists is an error, and the catalog table is left
   intact (the statement used to silently go to it).
5. `CREATE VIEW` over a catalogless table is an error (a permanent view with
   `RTE_TEMPRESULT` in `pg_rewrite` used to be created, and in a later
   transaction it bound to another table of the same name using the old
   column types).
6. A regular and a catalogless temp table with the same name —
   `already exists` both ways.
7. DROP + re-CREATE of the same name with a different row type in one
   transaction gives the correct result; DROP of a list with a catalogless
   and a catalog table removes both. (The review claimed that DROP does not
   reach the QEs — that was wrong: `ExecDropStmt` dispatches a copy of the
   original statement, and the QEs removed the entry through the same
   interception.)
8. DROP inside a savepoint is an error; after `ROLLBACK TO` the table is
   still there.
9. **Plan invalidation**: a plpgsql `SELECT count(*) FROM fx_c` in two
   transactions with different tables returns 5 and 7; a `DROP` from a
   cached plpgsql plan works on every call (`RemoveRelations` used to mutate
   `drop->objects`).
10. **Reader leak**: 3000 scans in one transaction (a plpgsql loop) pass;
    the reader is closed in `ExecEnd` instead of piling up until the end of
    the transaction (it used to cost +2 VFDs and a page window per scan).
11. `DISTRIBUTED REPLICATED`: the count is correct, a join with `pg_class`
    (Entry locus) reads the replica on one segment and pulls it with a
    Gather Motion — no scan on the QD; `DISTRIBUTED RANDOMLY` is correct;
    all rows on one segment — the other segment reads an empty store (the
    file is created on every writer); a join with a predicate on the key is
    not direct-dispatched.
12. 2PC (CTAS + INSERT into a heap table in one transaction) — the commit
    succeeds, `pgsql_tmp` is empty.

Separately, by hand: `kill -9` of a QE writer in the middle of a transaction
with a catalogless table → the segment reinitializes, the
`pgsql_tmp_CTMPRES_*` files are removed (before the fix `RemovePgTempFiles()`
ran only at postmaster start, so a crashed QE's files stayed until a full
restart; not re-checked live without the fix).

The smoke, plan and load tests were rerun on the new build: the results
match the previous ones (in the smoke output only the error texts changed:
`"poc_neg1" is a catalogless temp table` instead of `relation ... does not
exist`, and the new kill-switch HINT).

## History layout

The branch was rewritten layer by layer (the original history is kept in
`poc/idea1-catalogless-temp-ctas-backup-20260924`):

1. parsing of the `WITH (catalogless)` option and the GUC (a CTAS with the
   option says "not implemented yet");
2. session registry: hash, vid, name resolution, conflicts, DROP, cleanup
   (CTAS registers the metadata, the rows are discarded);
3. files: writing into the NTupleStore, opening/closing readers, cleanup
   after a crash;
4. planner: `RTE_TEMPRESULT`, the `TempResultScan` plan node, ORCA fallback,
   no views/rules;
5. executor: `TempResultScanState`, `nodeTempResultScan.c`, EXPLAIN;
6. tests and these notes.

Every commit builds from scratch (`make -C src/backend`, on macOS with the
environment fixes applied) without new compiler warnings.

On 2026-09-24 the branch was rebased from `9f785ada8a` onto
`origin/OPENGPDB_STABLE` (`9e1a945178`, 17 upstream commits newer), and the
macOS build-fix commit was taken out of the branch (see "Environment
fixes"). The only conflict was in `planner.c`: upstream added
`isQueryForOrca()`, and the RTE_TEMPRESULT rule moved there. After the
rebase the smoke, plan and fixes outputs are byte-identical and the load
test's result rows are identical.

## Diffstat

(relative to `origin/OPENGPDB_STABLE` at `9e1a945178`)

```
total:       66 files changed, 4247 insertions(+), 7 deletions(-)
src/ only:   57 files changed, 1866 insertions(+), 7 deletions(-)
```

Build: configure with the same options as the main tree (`--with-perl
--without-openssl --without-gssapi --with-libxml --without-mdblocales
--without-zstd --without-python --without-icu CFLAGS/CXXFLAGS='-O0 -g3'`);
`make -j8 -C src/backend` succeeds and links the `postgres` binary
(including ORCA, with the environment fixes above applied).
