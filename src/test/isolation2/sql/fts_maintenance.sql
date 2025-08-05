-- Faster FTS probes
-- Let FTS detect/declare failure sooner 
!\retcode gpconfig -c gp_fts_probe_interval -v 10 --masteronly;
!\retcode gpconfig -c gp_fts_probe_retries -v 2 --masteronly;
!\retcode gpconfig -c statement_timeout -v 2min;
!\retcode gpconfig -c gp_fts_maintenance -v on --skipvalidation --masteronly;
!\retcode gpstop -u;

include: helpers/server_helpers.sql;

-- Helper function
CREATE or REPLACE FUNCTION wait_until_segments_are_down(num_segs int)
RETURNS bool AS
$$
declare
retries int; /* in func */
begin /* in func */
  retries := 40; /* in func */
  loop /* in func */
    if (select count(*) = num_segs from gp_segment_configuration where status = 'd') then /* in func */
      return true; /* in func */
    end if; /* in func */
    if retries <= 0 then /* in func */
      return false; /* in func */
    end if; /* in func */
    perform pg_sleep(0.1); /* in func */
    retries := retries - 1; /* in func */
  end loop; /* in func */
end; /* in func */
$$ language plpgsql execute on master;

create table fts_mnt_tbl(i integer);
insert into fts_mnt_tbl select i from generate_series(1, 100)i;

-- CASE#1: mirror get's down when fts is in maintenance and is recovered
-- when maintenance is removed

-- no segment down.
select count(*) from gp_segment_configuration where status = 'd';

-- Kill a mirror segment
select pg_ctl((select datadir from gp_segment_configuration c
where c.role='m' and c.content=1), 'stop');

-- Take a nap to make sure FTS would notice a failed segment
select wait_until_segments_are_down(1);

-- Still no segmens down
select count(*) from gp_segment_configuration where status = 'd';

-- Even after a manual probe?
select gp_request_fts_probe_scan();
select count(*) from gp_segment_configuration where status = 'd';

-- Try read - non blocking
1:select count(*) from fts_mnt_tbl;

-- Try write - blocking
2&:insert into fts_mnt_tbl select i from generate_series(101, 200)i;

-- Try recovereseg - noop
!\retcode gprecoverseg -a;

-- Still no segmens down
select count(*) from gp_segment_configuration where status = 'd';

-- Still blocking
3&:insert into fts_mnt_tbl select i from generate_series(201, 300)i;

-- Turn maintenance off
!\retcode gpconfig -c gp_fts_maintenance -v off --skipvalidation --masteronly;
!\retcode gpstop -u;

-- writes unblock
2<:
3<:

-- gpsegment configuration changes
select count(*) from gp_segment_configuration where status = 'd';

-- gpsecoverseg works
!\retcode gprecoverseg -a;

-- gpsegment configuration changes
select count(*) from gp_segment_configuration where status = 'd';

1q:
2q:
3q:

-- CASE#2: mirror get's down when fts is in maintenance and is successfully recovered
-- with cluster restart
!\retcode gprecoverseg -ar;
!\retcode gpconfig -c gp_fts_maintenance -v on --skipvalidation --masteronly;
!\retcode gpstop -u;

-- Kill a mirror segment
select pg_ctl((select datadir from gp_segment_configuration c
where c.role='m' and c.content=1), 'stop');

-- Take a nap to make sure FTS would notice a failed segment
select wait_until_segments_are_down(1);

-- Lets check that after restart our cluster is working fine
!\retcode gpstop -ra -M fast;

-- both operations should be not blocking
4:select count(*) from fts_mnt_tbl;
5:insert into fts_mnt_tbl select i from generate_series(301, 400)i;

6:show gp_fts_maintenance;

-- CASE#3: mirror get's down when fts is in maintenance and is restarted manually

-- Kill a mirror segment
7:select pg_ctl((select datadir from gp_segment_configuration c
where c.role='m' and c.content=1), 'stop');

-- Take a nap to make sure FTS would notice a failed segment
8:select wait_until_segments_are_down(1);
9:select gp_request_fts_probe_scan();

-- should block
10&:insert into fts_mnt_tbl select i from generate_series(401, 500)i;

-- revive the mirror
11:select pg_ctl_start(datadir, port) from gp_segment_configuration where role = 'm' and content = 1;

-- should unblock
10<:

4q:
5q:
6q:
7q:
8q:
9q:
10q:
11q:

-- CASE#4: mirror is killed when fts is in maintenance and cannot start. GP stil does not mark it as down
-- in gp_segment_configuration

-- Kill a mirror segment
12:select pg_ctl((select datadir from gp_segment_configuration c
where c.role='m' and c.content=1), 'stop');

-- Guarantee that it won't start. Surely there is a better way which I didn't find
!\retcode rm -r `psql -Aqt -d postgres -c "select datadir from gp_segment_configuration where role = 'm' and content=1"`

13:select gp_request_fts_probe_scan();

-- gp segment configuration does not change
14:select count(*) from gp_segment_configuration where status = 'd';

-- Lets check that after restart our maintenance is working fine
!\retcode gpstop -ra -M fast;

-- gp segment configuration still does not change
15:select count(*) from gp_segment_configuration where status = 'd';

-- Try read - non blocking
16:select count(*) from fts_mnt_tbl;

-- Try write - blocking
17&:insert into fts_mnt_tbl select i from generate_series(501, 600)i;

-- noop
!\retcode gprecoverseg -Fa;

-- Turn maintenance off
!\retcode gpconfig -c gp_fts_maintenance -v off --skipvalidation --masteronly;
!\retcode gpstop -u;

-- should recover
17<:
-- fts detected the failed segment
19:select count(*) from gp_segment_configuration where status = 'd';
!\retcode gprecoverseg -Fa;

-- all good
20:select count(*) from gp_segment_configuration where status = 'd';

12q:
13q:
14q:
15q:
16q:
17q:
19q:
20q:

-- CASE#5: primary get's down when fts is in maintenance and reads/writes get errors. gprecoverseg only fixes
-- the problem when fts maintenance is lifted

!\retcode gpconfig -c gp_fts_maintenance -v on --skipvalidation --masteronly;
!\retcode gpstop -u;

-- no segment down.
21:select count(*) from gp_segment_configuration where status = 'd';

-- Kill a mirror segment
22:select pg_ctl((select datadir from gp_segment_configuration c
where c.role='p' and c.content=1), 'stop');

-- Take a nap to make sure FTS would notice a failed segment
23:select wait_until_segments_are_down(1);

-- Still no segmens down
24:select count(*) from gp_segment_configuration where status = 'd';

-- Try read - non blocking, failing
25:select count(*) from fts_mnt_tbl;

-- Try write - non blocking, failing
26:insert into fts_mnt_tbl select i from generate_series(601, 700)i;

-- Try recovereseg - noop
!\retcode gprecoverseg -a;

-- Still no segmens down
27:select count(*) from gp_segment_configuration where status = 'd';

-- Still failing
28:select count(*) from fts_mnt_tbl;

-- Turn maintenance off
!\retcode gpconfig -c gp_fts_maintenance -v off --skipvalidation --masteronly;
!\retcode gpstop -u;

-- gpsegment configuration changes
29:select wait_until_segments_are_down(1);
30:select count(*) from gp_segment_configuration where status = 'd';

-- gpsecoverseg works
!\retcode gprecoverseg -Fa;
!\retcode gprecoverseg -ra;
31:select count(*) from gp_segment_configuration where status = 'd';

32:drop table fts_mnt_tbl;

!\retcode gpconfig -r gp_fts_probe_retries --masteronly;
!\retcode gpconfig -r gp_fts_probe_interval --skipvalidation --masteronly;
!\retcode gpconfig -r gp_fts_maintenance --skipvalidation --masteronly;
!\retcode gpconfig -r statement_timeout;
!\retcode gpstop -u;
