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

#### 4. Other Metrics
-   **What:** Captures Instrument, Greenplum, System, Network, Interconnect, Spill metrics.
-   **GUC:** `gpsc.enable`.

### General Configuration
-   **Nested Queries:** When `gpsc.report_nested_queries` is `false`, only top-level queries are reported from the coordinator and segments, when `true`, both top-level and nested queries are reported from the coordinator, from segments collected as aggregates.
-   **Data Destination:** All collected data is sent to a Unix Domain Socket. Configure the path with `gpsc.uds_path`.
-   **User Filtering:** To exclude activity from certain roles, add them to the comma-separated list in `gpsc.ignored_users_list`.
-   **Trimming plans:** Query texts and execution plans are trimmed based on `gpsc.max_text_size` and `gpsc.max_plan_size` (default: 1024KB). For now, it is not recommended to set these GUCs higher than 1024KB.
-   **Analyze collection:** Analyze is sent if execution time exceeds `gpsc.min_analyze_time`, which is 10 seconds by default. Analyze is collected if `gpsc.enable_analyze` is true.
