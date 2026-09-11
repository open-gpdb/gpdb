# ast_log

This extension provides detailed logging of Query Abstract Syntax Trees (AST) for Greenplum Database.

Unlike standard statement logging which records the raw SQL text, `ast_log` captures the internal parsed representation of queries as seen by the planner. This provides a structured view of the query logic, including target lists, join trees, and qualifications, which is essential for deep query analysis, optimizer debugging, and automated query understanding.

The `ast_log` module hooks into the planner and utility execution phases to intercept queries and log their AST representation to a dedicated log file. It is designed to be lightweight and compatible with Greenplum 6 (PostgreSQL 9.4).

## Key Features

- **AST Logging**: Logs the internal node structure of `INSERT ... SELECT` statements (plain `INSERT ... VALUES` without a data source is not logged).
- **CTAS Support**: Specifically handles `CREATE TABLE AS SELECT` (including `SELECT INTO` and `CREATE MATERIALIZED VIEW`) to log the AST of the underlying query separately from the DDL command.
- **Granular Control**: Each class of statements (CTAS, INSERT) is controlled by its own independent GUC.
- **Greenplum Aware**: Runs exclusively on the Coordinator (Master) node, avoiding noise on segment nodes.
- **Dedicated Log File**: Writes audit records to `pg_log/ast.log` with timestamps and session information.

## Log Format

Each line in `pg_log/ast.log` is a CSV record with the following fields:

- Timestamp (`YYYY-MM-DD HH:MM:SS.uuuuuu`)
- Session ID (`gp_session_id`)
- Statement class: `CTAS` or `INSERT`
- Fully-qualified target object name (e.g. `public.mytable`)
- Serialized query AST with a dictionary of referenced tables/functions


## Configuration Parameters

The following parameters can be configured via `SET` command. All settings require superuser privileges.

### ast_log.ctas

Enables logging of the AST for the query part of `CREATE TABLE AS SELECT` (also covers `SELECT INTO` and `CREATE MATERIALIZED VIEW`).

Default: `off`.

### ast_log.insert

Enables logging of the AST for `INSERT ... SELECT` statements.

Default: `off`.

### Example:

```
SET ast_log.ctas = on;
SET ast_log.insert = on;
```

## Installation

Load the extension in the current session:

```
LOAD '$libdir/ast_log';
```

Or load automatically at server start (Coordinator only):

```
gpconfig -c shared_preload_libraries -v '' -m 'ast_log'
gpstop -ra
```
