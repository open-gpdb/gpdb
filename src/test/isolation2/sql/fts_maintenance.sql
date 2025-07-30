-- Faster FTS probes
-- Let FTS detect/declare failure sooner 
!\retcode gpconfig -c gp_fts_probe_interval -v 10 --masteronly;
!\retcode gpconfig -c gp_fts_probe_retries -v 2 --masteronly;

!\retcode gpconfig -c gp_fts_maintenance -v on --skipvalidation --masteronly;
!\retcode gpstop -u;

include: helpers/server_helpers.sql;

create table fts_mnt_tbl(i integer);
insert into fts_mnt_tbl select i from generate_series(1, 100)i;

-- no segment down.
select count(*) from gp_segment_configuration where status = 'd';

-- Kill a mirror segment
select pg_ctl((select datadir from gp_segment_configuration c
where c.role='m' and c.content=1), 'stop');

-- Take a nap to make sure FTS would notice a failed segment
select pg_sleep(0.2);

-- Still no segmens down
select count(*) from gp_segment_configuration where status = 'd';

-- Even after a manual probe?
select gp_request_fts_probe_scan();
select count(*) from gp_segment_configuration where status = 'd';

-- Try read - non blocking
1:select count(*) from fts_mnt_tbl;

-- Try write - blocking
1&:insert into fts_mnt_tbl select i from generate_series(101, 200)i;

-- Try recovereseg - noop
!\retcode gprecoverseg -a;

-- Still no segmens down
select count(*) from gp_segment_configuration where status = 'd';

-- Still blocking
2&:insert into fts_mnt_tbl select i from generate_series(201, 300)i;

-- Turn maintenance off
!\retcode gpconfig -c gp_fts_maintenance -v off --skipvalidation --masteronly;
!\retcode gpstop -u;

-- writes unblock
1<:
2<:

-- gpsegment configuration changes
select count(*) from gp_segment_configuration where status = 'd';

-- gpsecoverseg works
!\retcode gprecoverseg -a;

-- gpsegment configuration changes
select count(*) from gp_segment_configuration where status = 'd';

!\retcode gprecoverseg -ar;

