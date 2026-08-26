# ast_log

This extension provides detailed logging of Query Abstract Syntax Trees (AST) for Greenplum Database.

Unlike standard statement logging which records the raw SQL text, `ast_log` captures the internal parsed representation of queries as seen by the planner. This provides a structured view of the query logic, including target lists, join trees, and qualifications, which is essential for deep query analysis, optimizer debugging, and automated query understanding.

The `ast_log` module hooks into the planner and utility execution phases to intercept queries and log their AST representation to a dedicated log file. It is designed to be lightweight and compatible with Greenplum 6 (PostgreSQL 9.4).

## Key Features

- **AST Logging**: Logs the internal node structure of `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements.
- **CTAS Support**: Specifically handles `CREATE TABLE AS SELECT` to log the AST of the underlying query separately from the DDL command.
- **Granular Control**: Allows enabling logging for specific classes of statements (e.g., only AST for modifications, or only CTAS).
- **Greenplum Aware**: Runs exclusively on the Master/Coordinator node, avoiding noise on segment nodes.
- **Dedicated Log File**: Writes audit records to `pg_log/ast.log` with timestamps and session information.

## Configuration Parameters

The following parameters can be configured in `postgresql.conf` or via `ALTER SYSTEM`. All settings require superuser privileges.

### ast_log.log
Specifies which classes of statements will be logged. Multiple classes can be provided using a comma-separated list. Classes can be subtracted by prefacing them with a `-` sign.
Possible values:
- **ast_sel**: AST for `SELECT` statements.
- **ast_mod**: AST for `INSERT`, `UPDATE`, `DELETE` statements.
- **ast_ctas**: AST for the query part of `CREATE TABLE AS SELECT`.
- **all**: Includes all above classes.
- **none**: Disables logging (default).

### ast_log.log_client
Specifies whether log messages are also sent to the client as notices. This is intended for regression testing and debugging. 

Default is `off` (messages go only to the log file).

## How to create the extension

Add ast_log to shared_preload_libraries and restart the cluster.

```
gpconfig -c shared_preload_libraries -v 'ast_log'
gpstop -ra
```

Create the extension in your database.

```
CREATE EXTENSION ast_log;
```

Configure the desired log classes (e.g., to log AST for selects and modifications):

```
SET ast_log.log = 'ast_sel, ast_mod, ast_ctas';
```

Audit entries will now be written to the file `pg_log/ast.log` with the prefix `AUDIT:`. If you also want to see them in the client (for testing), set:

```
SET ast_log.log_client = ON;
```
