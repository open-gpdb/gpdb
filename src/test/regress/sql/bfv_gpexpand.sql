-- Check that it is possible to add columns to AOCO tables in the period
-- between adding new segments to a cluster and expanding the tables.

-- start_ignore
drop table if exists t, tp, tsp;
\c postgres
drop schema if exists gpexpand, gpshrink cascade;
\c regression
\! rm -rf /tmp/datadirs/dbfast_mirror4/ /tmp/datadirs/dbfast4/
-- end_ignore

-- Create table without partitions
create table t (i int, j int)
with (appendonly = true, orientation = column)
distributed by (i);

-- Create table with partitions
create table tp (i int, j int)
with (appendonly = true, orientation = column)
distributed by (i)
partition by range (j) (partition p start (0) end (2) every(1));

-- Create table with subpartitions
create table tsp (i int, j int, k int)
with (appendonly = true, orientation = column)
distributed by (i)
partition by range (j)
  subpartition by range (k)
  subpartition template (subpartition sp start (0) end (2) every(1))
(partition p start (0) end (2) every(1));

-- Add tuples to the tables to fill their pg_aocsseg-s on master
insert into t (i, j) values (1, 1);
insert into tp (i, j) values (1, 1);
insert into tsp (i, j, k) values (1, 1, 1);

-- Check that pg_aocsseg-s contain tuples
select string_agg('table ' || segrelid::regclass::text, ' union all ')
         || ';' show_pg_aocssegs
  from pg_appendonly
 where relid in ('t'::regclass,
                 'tp_1_prt_p_2'::regclass,
                 'tsp_1_prt_p_2_2_prt_sp_2'::regclass) \gset

:show_pg_aocssegs

-- Add new segments
\! echo "localhost|localhost|7008|/tmp/datadirs/dbfast4/demoDataDir3|9|3|p" > /tmp/testexpand
\! echo "localhost|localhost|7009|/tmp/datadirs/dbfast_mirror4/demoDataDir3|10|3|m" >> /tmp/testexpand

-- start_ignore
\! gpexpand -i /tmp/testexpand
-- end_ignore

-- Add columns to the tables
alter table t add b boolean;
alter table tp add b boolean;
alter table tsp add b boolean;

-- Expand tables, no error must happen
-- \! gpexpand | grep ERROR | wc -l
\! gpexpand

-- Check that the tables are expanded
select numsegments
  from gp_distribution_policy
 where localoid in ('t'::regclass,
                    'tp_1_prt_p_2'::regclass,
                    'tsp_1_prt_p_2_2_prt_sp_2'::regclass);

\c postgres
select status
  from gpexpand.status_detail
 where dbname = 'regression'
   and fq_name in ('public.t',
                   'public.tp_1_prt_p_2',
                   'public.tsp_1_prt_p_2_2_prt_sp_2');
\c regression


-- Cleanup
-- start_ignore
\! gpshrink -i /tmp/testexpand
\! gpshrink -i /tmp/testexpand
-- end_ignore

-- Check that cluster is shrunk
select count(*) from gp_segment_configuration;

drop table t, tp, tsp;
\c postgres
drop schema gpexpand, gpshrink cascade;
\c regression

\! rm -rf /tmp/datadirs/dbfast_mirror4/ /tmp/datadirs/dbfast4/ /tmp/testexpand
