-- start_ignore
drop external table if exists access_log;
drop table if exists t_heap_part, t_heap, t_ao_part, t_ao, t_aoco_part, t_aoco;
-- Delete log files from master
select pg_file_unlink('pg_log/access.log');
-- Delete log files from segments
select pg_file_unlink('pg_log/access.log') from gp_dist_random('gp_id');
-- end_ignore

-- Create external table to read log files from segments
do
$$ 
declare
  seg text;
  locations text := '';
begin 
  for seg in
    select hostname || ':' || port || datadir
      from gp_segment_configuration
     where role = 'p' and content >= 0
  loop
    if locations != '' then
      locations = locations || ',';
    end if;

    locations = locations || '''file://' || seg || '/pg_log/access.log''';
  end loop;

  execute 'create external table access_log (logtime timestamp with time zone, loguser text, tbl text)
location (' || locations || ') format ''csv''';
end $$;

-- Start logging
load '$libdir/access_log.so';


create or replace function show_log(before_query text)
  returns table(gp_segment_id int, date_ok bool, user_ok bool, tbl text)
as $$
  select gp_segment_id,
    logtime between before_query::timestamp with time zone and now() date_ok,
    loguser=current_user user_ok,
    tbl
  from access_log;
$$ language sql;


--
-- Heap table

-- Partitioned table
create table t_heap_part(a int, b int, c int)
distributed by (a)
partition by range (b)
  subpartition by range (c)
    subpartition template (start (40) end (46) every (3))
(start (0) end (4) every (2));

-- All partitions
select now() as before_query \gset
select * from t_heap_part;
select * from show_log(:'before_query');
select pg_file_unlink('pg_log/access.log') from gp_dist_random('gp_id');

-- Read partitions selected by condition in WHERE
select now() as before_query \gset
select * from t_heap_part where c = 40;
select * from show_log(:'before_query');
select pg_file_unlink('pg_log/access.log') from gp_dist_random('gp_id');

select now() as before_query \gset
select * from t_heap_part where b = 0;
select gp_segment_id, loguser=current_user user_ok, tbl from access_log;
-- Don't delete files, because the next select will lead to adding log records
-- on one segment only. Reading from an external table fails when log file does
-- not exist on any segment.

-- One segment
select now() as before_query \gset
select * from t_heap_part where a is null;
select * from show_log(:'before_query');
select pg_file_unlink('pg_log/access.log') from gp_dist_random('gp_id');


-- Table without partitions
create table t_heap(a int)
distributed by (a);

select now() as before_query \gset
select * from t_heap;
select * from show_log(:'before_query');
select pg_file_unlink('pg_log/access.log') from gp_dist_random('gp_id');


--
-- AO table

-- Partitioned table
create table t_ao_part(a int, b int, c int)
with (appendonly = true)
distributed by (a)
partition by range (b)
  subpartition by range (c)
    subpartition template (start (40) end (46) every (3))
(start (0) end (4) every (2));

-- All partitions
select now() as before_query \gset
select * from t_ao_part;
select * from show_log(:'before_query');
select pg_file_unlink('pg_log/access.log') from gp_dist_random('gp_id');

-- Read partitions selected by condition in WHERE
select now() as before_query \gset
select * from t_ao_part where c = 40;
select * from show_log(:'before_query');
select pg_file_unlink('pg_log/access.log') from gp_dist_random('gp_id');

select now() as before_query \gset
select * from t_ao_part where b = 0;
select gp_segment_id, loguser=current_user user_ok, tbl from access_log;

-- One segment
select now() as before_query \gset
select * from t_ao_part where a is null;
select * from show_log(:'before_query');
select pg_file_unlink('pg_log/access.log') from gp_dist_random('gp_id');


-- Table without partitions
create table t_ao(a int, b int)
distributed by (a);

select now() as before_query \gset
select * from t_ao;
select * from show_log(:'before_query');
select pg_file_unlink('pg_log/access.log') from gp_dist_random('gp_id');


--
-- AOCO table

-- Partitioned table
create table t_aoco_part(a int, b int, c int)
with (appendonly = true, orientation = column)
distributed by (a)
partition by range (b)
  subpartition by range (c)
    subpartition template (start (40) end (46) every (3))
(start (0) end (4) every (2));

-- All partitions
select now() as before_query \gset
select * from t_aoco_part;
select * from show_log(:'before_query');
select pg_file_unlink('pg_log/access.log') from gp_dist_random('gp_id');

-- Read partitions selected by condition in WHERE
select now() as before_query \gset
select * from t_aoco_part where c = 40;
select * from show_log(:'before_query');
select pg_file_unlink('pg_log/access.log') from gp_dist_random('gp_id');

select now() as before_query \gset
select * from t_aoco_part where b = 0;
select gp_segment_id, loguser=current_user user_ok, tbl from access_log;

-- One segment
select now() as before_query \gset
select * from t_aoco_part where a is null;
select * from show_log(:'before_query');
select pg_file_unlink('pg_log/access.log') from gp_dist_random('gp_id');


-- Table without partitions
create table t_aoco(a int, b int)
distributed by (a);

select now() as before_query \gset
select * from t_aoco;
select * from show_log(:'before_query');
select pg_file_unlink('pg_log/access.log') from gp_dist_random('gp_id');


--
-- Check user name logging

-- start_ignore
drop role if exists user1;
-- end_ignore
create role user1 login resource queue pg_default;
grant select on t_ao_part to user1;
grant select on t_aoco to user1;

select '\! cp "' || setting || '/pg_hba.conf" "'  || setting || '/pg_hba.conf.backup"' as cp_backup
from pg_settings
where name = 'data_directory' \gset

:cp_backup

select '\! echo "local all user1 trust" >> ' || setting || '/pg_hba.conf' as add_user
from pg_settings
where name = 'data_directory' \gset

:add_user

select current_user \gset
-- start_ignore
\! gpconfig -c shared_preload_libraries -v 'access_log' -m ''
\! gpstop -raiq
-- end_ignore
\c - user1

select * from t_ao_part;
select * from t_aoco;

\c - :"current_user"

select gp_segment_id, loguser, tbl from access_log;

--
-- Cleanup
drop function show_log(text);
drop external table access_log;
drop table t_heap_part, t_heap, t_ao_part, t_ao, t_aoco_part, t_aoco;
-- start_ignore
select '\! cp "' || setting || '/pg_hba.conf.backup" "' || setting || '/pg_hba.conf"' as cp_restore
from pg_settings
where name = 'data_directory' \gset

:cp_restore

select pg_file_unlink('pg_log/access.log') from gp_dist_random('gp_id');
select pg_file_unlink('pg_log/access.log');

\! gpconfig -r shared_preload_libraries
\! gpstop -raiq
-- end_ignore
