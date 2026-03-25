# access_log

The extension logs when and which user initializes Seq Scan on which partition
or table. The log file name is pg_log/access.log. To activate the extension you
can use the shared_preload_libraries GUC or the LOAD command.

If you want to register Seq Scans on segments only you should load access_log
on segments and don't load on master:

```
gpconfig -c shared_preload_libraries -v 'access_log' -m ''
```
