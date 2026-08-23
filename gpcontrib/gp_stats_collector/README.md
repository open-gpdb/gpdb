## GP Stats Collector

An extension for collecting greenplum query execution metrics and reporting them to an external agent.

### Collected Statistics

#### 1. Query Lifecycle
-   **What:** Captures query text, normalized query text, timestamps (submit, start, end, done), and user/database info.
-   **GUC:** `gpsc.enable`.

#### 2. `EXPLAIN` data
-   **What:** Triggers generation of the `EXPLAIN (TEXT, COSTS, VERBOSE)` and captures it.
-   **GUC:** `gpsc.enable`.

#### 3. `EXPLAIN ANALYZE` data
-   **What:** Triggers generation of the `EXPLAIN (TEXT, ANALYZE, BUFFERS, TIMING, VERBOSE)` and captures it.
-   **GUCs:** `gpsc.enable`, `gpsc.min_analyze_time`, `gpsc.enable_cdbstats`(ANALYZE), `gpsc.enable_analyze`(BUFFERS, TIMING, VERBOSE).

#### 4. `EXPLAIN (FORMAT JSON)` data
-   **What:** In addition to the text plans, captures the structured `EXPLAIN (FORMAT JSON)` payloads: `plan_json` at query start and `analyze_json` at query end.
-   **GUC:** `gpsc.enable_json_plan` (default `off`).
-   **Why not derive it from the text plan:** the text plan is a rendering, not a format.
    Its tree structure is encoded in indentation, and Greenplum adds annotations of its own
    (`Gather Motion 2:1 (slice1; segments: 2)`, `Rows out: Avg ... x N workers`, ORCA cost
    lines, `Executor memory`). Tools that parse text plans (PEV2 `fromText`) silently lose
    the `Motion` nodes and their subtrees, and the per-segment ANALYZE figures are prose.
    A text parser would be reverse engineering of `explain.c` output for one Greenplum
    version, while `EXPLAIN (FORMAT JSON)` is produced by the same tree walk and stays
    stable across versions. The text plan remains the source of `plan_id`.

#### 5. Other Metrics
-   **What:** Captures Instrument, Greenplum, System, Network, Interconnect, Spill metrics.
-   **GUC:** `gpsc.enable`.

### General Configuration
-   **Nested Queries:** When `gpsc.report_nested_queries` is `false`, only top-level queries are reported from the coordinator and segments, when `true`, both top-level and nested queries are reported from the coordinator, from segments collected as aggregates.
-   **Data Destination:** All collected data is sent to a Unix Domain Socket. Configure the path with `gpsc.uds_path`.
-   **User Filtering:** To exclude activity from certain roles, add them to the comma-separated list in `gpsc.ignored_users_list`.
-   **Trimming plans:** Query texts and execution plans are trimmed based on `gpsc.max_text_size` and `gpsc.max_plan_size` (default: 1024KB). For now, it is not recommended to set these GUCs higher than 1024KB.
-   **Analyze collection:** Analyze is sent if execution time exceeds `gpsc.min_analyze_time`, which is 10 seconds by default. Analyze is collected if `gpsc.enable_analyze` is true.
-   **JSON plans:** `gpsc.enable_json_plan` (default `off`, `PGC_SUSET`) additionally collects the structured `EXPLAIN (FORMAT JSON)` plan. Consumers that render the plan graphically (e.g. PEV2) need it: the text plan does not expose Motion nodes in a machine-readable form. When it is `on`, `plan_json` is built at the start of *every* query — a second `ExplainPrintPlan` pass — and `analyze_json` is built at the end under the same rules as `analyze_text` (`gpsc.enable_analyze` plus the `gpsc.min_analyze_time` threshold), so only long queries pay for it. The text plans are unaffected: they are always collected, and `plan_id` is still the hash of the normalized *text* plan. JSON is noticeably larger than text, so `gpsc.max_plan_size` matters more here — a JSON payload that does not fit the limit is dropped whole rather than truncated (a truncated JSON is unparseable), while the text plan is trimmed as usual. In `gpsc.logging_mode = 'TBL'` the payloads land in the `plan_json` / `analyze_json` columns of `gpsc.log`; the columns were added in extension version 1.2, so an installation created with 1.1 needs `ALTER EXTENSION gp_stats_collector UPDATE` to get them. Until that is done the extension detects the column-count mismatch, emits a one-time `WARNING` per backend and skips table logging instead of writing misaligned rows.
