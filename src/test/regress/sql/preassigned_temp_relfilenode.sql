--
-- Test gp_enable_preassigned_temp_relfilenode: relfilenodes of temp
-- relations are assigned on the coordinator and dispatched to the segments,
-- so a temp relation has the same relfilenode on all nodes.
--
-- Relfilenodes of all relations in the session's temp namespaces, except
-- sequences (see below), and of the AO auxiliary relations of the given
-- tables, on the coordinator and on all segments.
create or replace function temp_rfn(tables regclass[])
returns table(relname name, gp_segment_id int, relfilenode oid) as $$
  with rels as (
    select c.oid from pg_class c
     where c.relnamespace in (pg_my_temp_schema(),
                              (select t.oid from pg_namespace n, pg_namespace t
                                where n.oid = pg_my_temp_schema()
                                  and t.nspname = replace(n.nspname, 'pg_temp', 'pg_toast_temp')))
       and c.relkind <> 'S'
    union all
    select unnest(array[segrelid, blkdirrelid, visimaprelid])
      from pg_appendonly where relid = any(tables)
  )
  select c.relname, -1, c.relfilenode from pg_class c where c.oid in (select oid from rels)
  union all
  select c.relname, c.gp_segment_id, c.relfilenode from gp_dist_random('pg_class') c
   where c.oid in (select oid from rels)
$$ language sql;

-- Report, per relation name pattern, whether the relfilenode is the same on
-- all nodes, and whether it comes from the reserved temp range.
create or replace function check_temp_rfn(tables regclass[])
returns table(relname text, same_on_all_nodes bool, in_temp_range bool) as $$
  select regexp_replace(relname, '[0-9]+', 'N', 'g'),
         count(distinct relfilenode) = 1 and count(*) = 1 + (select count(*) from gp_segment_configuration where role = 'p' and content >= 0),
         min(relfilenode) >= 1073741824
    from temp_rfn(tables) group by relname order by 1
$$ language sql;

set gp_enable_preassigned_temp_relfilenode = on;

-- heap table, with toast table, primary key and an index
create temp table tr_heap(a int primary key, b text) distributed by (a);
create index tr_heap_b on tr_heap(b);
insert into tr_heap select g, repeat('x', 5000) || g from generate_series(1, 100) g;
select count(*), sum(length(b)) from tr_heap;

-- AO and AOCO tables, with their auxiliary relations
create temp table tr_ao(a int, b text) with (appendonly = true) distributed by (a);
create index tr_ao_a on tr_ao(a);
create temp table tr_aoco(a int, b text) with (appendonly = true, orientation = column) distributed by (a);
insert into tr_ao select g, 'v' || g from generate_series(1, 1000) g;
insert into tr_aoco select g, 'v' || g from generate_series(1, 1000) g;
delete from tr_ao where a <= 10;
select count(*) from tr_ao;
select count(*) from tr_aoco;

-- CREATE TABLE AS
create temp table tr_ctas as select a, b from tr_aoco distributed by (a);
select count(*) from tr_ctas;

-- create, drop and create again under the same name in one transaction
begin;
create temp table tr_tx(a int) distributed by (a);
drop table tr_tx;
create temp table tr_tx(a int) distributed by (a);
insert into tr_tx values (1), (2), (3);
commit;
select count(*) from tr_tx;

select * from check_temp_rfn(array['tr_ao', 'tr_aoco']::regclass[]);

-- Number of relations whose relfilenode differs across nodes, or is not
-- from the temp range.
create or replace function count_bad_temp_rfn(tables regclass[])
returns bigint as $$
  select count(*) from check_temp_rfn(tables)
   where not (same_on_all_nodes and in_temp_range)
$$ language sql;

-- Files of this database with a relfilenode from the temp range, on the
-- coordinator and on all segments, and whether a relation in pg_class of the
-- same node has that relfilenode; the files without one are orphaned.  Temp
-- relation files are named t_<relfilenode>[_fork][.segno].
create or replace view temp_range_files as
  select pg_catalog.gp_execution_segment() as gp_segment_id, f as filename,
         substring(f from '^t_([0-9]+)')::bigint as relfilenode
    from pg_ls_dir('base/' || (select oid from pg_database
                                where datname = current_database())) f
   where f ~ '^t_[0-9]+'
     and substring(f from '^t_([0-9]+)')::bigint >= 1073741824;
create or replace function all_temp_range_files()
returns table(gp_segment_id int, filename text, in_pg_class bool) as $$
  select f.gp_segment_id, f.filename, r.relfilenode is not null
    from (select * from temp_range_files
          union all
          select * from gp_dist_random('temp_range_files')) f
    left join (select distinct -1 as gp_segment_id, relfilenode::bigint as relfilenode
                 from pg_class
               union
               select distinct gp_segment_id, relfilenode::bigint
                 from gp_dist_random('pg_class')) r
      on r.gp_segment_id = f.gp_segment_id and r.relfilenode = f.relfilenode
$$ language sql;
create or replace function temp_range_orphan_files()
returns table(gp_segment_id int, filename text) as $$
  select gp_segment_id, filename from all_temp_range_files() where not in_pg_class
$$ language sql;

-- Operations that give an existing relation a new relfilenode
truncate tr_heap;
insert into tr_heap select g, repeat('y', 5000) || g from generate_series(1, 50) g;
truncate tr_ao, tr_aoco;
insert into tr_ao select g, 'w' || g from generate_series(1, 500) g;
insert into tr_aoco select g, 'w' || g from generate_series(1, 500) g;
select count_bad_temp_rfn(array['tr_ao', 'tr_aoco']::regclass[]) as after_truncate;

begin;
truncate tr_ctas;
truncate tr_ctas;
insert into tr_ctas values (1, 'a');
commit;
select count_bad_temp_rfn(array['tr_ao', 'tr_aoco']::regclass[]) as after_truncate_twice;

reindex index tr_heap_b;
reindex table tr_heap;
reindex table tr_ao;
select count_bad_temp_rfn(array['tr_ao', 'tr_aoco']::regclass[]) as after_reindex;

vacuum full tr_heap;
vacuum full tr_ao;
vacuum full tr_aoco;
select count_bad_temp_rfn(array['tr_ao', 'tr_aoco']::regclass[]) as after_vacuum_full;

cluster tr_heap using tr_heap_pkey;
alter table tr_heap add column c int;
alter table tr_heap alter column c type bigint;
alter table tr_ao set distributed by (b);
select count_bad_temp_rfn(array['tr_ao', 'tr_aoco']::regclass[]) as after_rewrite;

-- A serial column.  TRUNCATE ... RESTART IDENTITY resets the sequence only
-- on the coordinator, after the command has been dispatched, so the
-- sequence gets a local relfilenode there.
create temp table tr_serial(id serial, a int) distributed by (a);
insert into tr_serial(a) select generate_series(1, 10);
select count(distinct relfilenode) = 1 as seq_same_before_restart
  from (select relfilenode from pg_class where relname = 'tr_serial_id_seq'
        union all
        select relfilenode from gp_dist_random('pg_class') where relname = 'tr_serial_id_seq') s;
truncate tr_serial restart identity;
insert into tr_serial(a) values (1);
select id from tr_serial;
select count(distinct relfilenode) = 1 as seq_same_after_restart
  from (select relfilenode from pg_class where relname = 'tr_serial_id_seq'
        union all
        select relfilenode from gp_dist_random('pg_class') where relname = 'tr_serial_id_seq') s;

-- An attempt to create a table that fails on the QD before the command is
-- dispatched leaves an assignment behind; it must not be used for the next
-- table of the same name.
begin;
savepoint sp;
create temp table tr_sp(a int, b int default 'x') distributed by (a);
rollback to sp;
create temp table tr_sp(a int, b int) distributed by (a);
insert into tr_sp values (1, 1);
commit;
select count_bad_temp_rfn(array['tr_ao', 'tr_aoco']::regclass[]) as after_savepoint;

select count(*) from tr_heap;
select count(*) from tr_ao;
select count(*) from tr_aoco;
select count(*) from tr_ctas;
select count(*) from tr_sp;

-- None of the above leaves files behind.  The files of the live relations
-- are seen on every node.
select count(distinct gp_segment_id) = (select count(*) from gp_segment_configuration
                                         where role = 'p') as files_seen_on_all_nodes
  from all_temp_range_files() where in_pg_class;
select * from temp_range_orphan_files();

-- Neither do aborted transactions: new relations, and new relfilenodes of
-- existing ones, rolled back as a whole or to a savepoint
begin;
create temp table tr_abort(a int, b text) with (appendonly = true) distributed by (a);
create index tr_abort_a on tr_abort(a);
insert into tr_abort select g, 'x' || g from generate_series(1, 100) g;
truncate tr_heap;
rollback;
begin;
create temp table tr_abort(a int primary key, b text) distributed by (a);
insert into tr_abort select g, repeat('x', 5000) from generate_series(1, 10) g;
select 1 / 0;
rollback;
begin;
savepoint sp;
truncate tr_ao;
create temp table tr_abort(a int) distributed by (a);
rollback to sp;
commit;
select * from temp_range_orphan_files();
select count(*) from tr_heap;
select count(*) from tr_ao;

-- VACUUM of a table with a bitmap index rebuilds the index only on the nodes
-- with dead tuples, so that rebuild uses local relfilenodes, which differ
-- across the nodes.
create temp table tr_bm(a int, b int) distributed by (a);
create index tr_bm_b on tr_bm using bitmap(b);
insert into tr_bm select g, g % 10 from generate_series(1, 1000) g;
select count_bad_temp_rfn(array[]::regclass[]) as bitmap_before_vacuum;
delete from tr_bm where a % 2 = 0;
vacuum tr_bm;
select count(*) from tr_bm where b = 1;
select regexp_replace(c.relname, '[0-9]+', 'N', 'g') as relname,
       (select count(distinct x.relfilenode) = 1
          from (select relfilenode from pg_class where oid = c.oid
                union all
                select relfilenode from gp_dist_random('pg_class') where oid = c.oid) x) as same_on_all_nodes
  from pg_class c
 where c.oid in ('tr_bm'::regclass, 'tr_bm_b'::regclass)
    or c.relname in ('pg_bm_' || 'tr_bm_b'::regclass::oid,
                     'pg_bm_' || 'tr_bm_b'::regclass::oid || '_index')
 order by 1;
reindex index tr_bm_b;
select count_bad_temp_rfn(array[]::regclass[]) as bitmap_after_reindex;
drop table tr_bm;
select * from temp_range_orphan_files();

-- Permanent tables are not affected
create table tr_perm(a int) distributed by (a);
select max(relfilenode) < 1073741824 as below_temp_range
  from (select relfilenode from pg_class where relname = 'tr_perm'
        union all
        select relfilenode from gp_dist_random('pg_class') where relname = 'tr_perm') s;
drop table tr_perm;

-- With the GUC off, temp relfilenodes are not taken from the temp range
set gp_enable_preassigned_temp_relfilenode = off;
create temp table tr_off(a int) distributed by (a);
select bool_or(relfilenode >= 1073741824) as any_in_temp_range
  from temp_rfn(array[]::regclass[]) where relname = 'tr_off';

-- The end of the session drops its temp relations on all nodes, and their
-- files with them: no file from the temp range is left, orphaned or not.
-- The old backends clean up asynchronously, so wait.
\c
create or replace function wait_no_temp_range_files() returns bigint as $$
declare
  n bigint;
begin
  for i in 1 .. 600 loop
    select count(*) into n from all_temp_range_files();
    exit when n = 0;
    perform pg_sleep(0.1);
  end loop;
  return n;
end
$$ language plpgsql;
select wait_no_temp_range_files() as temp_range_files_after_session_end;

drop function wait_no_temp_range_files();
drop function temp_range_orphan_files();
drop function all_temp_range_files();
drop view temp_range_files;
drop function count_bad_temp_rfn(regclass[]);
drop function check_temp_rfn(regclass[]);
drop function temp_rfn(regclass[]);
