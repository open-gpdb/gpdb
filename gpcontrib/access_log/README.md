# access_log

The extension logs access to Greenplum tables and partitions.

For read operations, it logs when a sequential scan is initialized, which
user initiated it, which Greenplum session it belongs to, and which table
or partition is scanned.

For write operations, it logs when an `INSERT`, `UPDATE`, or `DELETE`
operation starts on a target table. For a partitioned target, the relation
logged by the executor is used; depending on the operation, this can be the
root target table or an affected partition.

Each log record contains:

1. event timestamp;
2. user name;
3. Greenplum session ID;
4. schema-qualified table or partition name;
5. operation type:
   - `read` for sequential scan initialization;
   - `write` for `INSERT`, `UPDATE`, or `DELETE`.

The log file is `pg_log/access.log`.

If you want to register table accesses on segments only you should load
access_log on segments and don't load on master:

```
gpconfig -c shared_preload_libraries -v 'access_log' -m ''
```
