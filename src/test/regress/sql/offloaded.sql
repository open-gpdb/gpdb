-- start_ignore
drop table if exists t, t1, t2, t3, t4, t5, t6, t7, t8, t9;
-- end_ignore

-- Default value for offloaded is false
reset gp_default_storage_options;
show gp_default_storage_options;

-- The offloaded option is valid for AO relations only.
create table t(i int) with(offloaded) distributed by(i);


--
-- AORO

-- offloaded is not a column specific option
create table t(i int encoding(offloaded)) with(appendonly) distributed by(i);

-- Invalid value
create table t(i int) with(appendonly, offloaded=invalid_value) distributed by(i);

-- Set offloaded to true
create table t1(i int) with(appendonly, offloaded) distributed by(i);
create table t2(i int) with(appendonly, offloaded=true) distributed by(i);
create table t3(i int) with(appendonly, offloaded=1) distributed by(i);
create table t4(i int) with(appendonly, OFFLOADED=TRUE) distributed by(i);

-- Set offloaded to false
create table t5(i int) with(appendonly, OFFLOADED=0) distributed by(i);
create table t6(i int) with(appendonly, offloaded=false) distributed by(i);

-- We can set offloaded by default
set gp_default_storage_options='appendonly=false,blocksize=32768,compresstype=none,checksum=true,orientation=row,offloaded=true';
show gp_default_storage_options;

-- When offloaded is set by default it affects AO tables, but not heap ones
create table t7(i int) with(appendonly) distributed by(i);
create table t8(i int) distributed by(i);
reset gp_default_storage_options;

-- Partitions and subpartitions are offloaded when the table is created as offloaded
create table t9(id bigint, d date, r char(1), s numeric(18,2))
with(appendonly, offloaded)
distributed by (id)
partition by range (d)
subpartition by list (r)
subpartition template (
  subpartition north values ('N'),
  subpartition south values ('S')
)
(
  partition p2024 start (date '2024-01-01') end (date '2025-01-01'),
  partition p2025 start (date '2025-01-01') end (date '2026-01-01'),
  partition p2026 start (date '2026-01-01') end (date '2027-01-01')
);

-- Added partitions and subpartitions are not offloaded by default
alter table t9
add partition p2027 start (date '2027-01-01') end (date '2028-01-01');

-- We can add offloaded partitions
alter table t9
add partition p2028 start (date '2028-01-01') end (date '2029-01-01')
with(appendonly, offloaded);

-- Check reloptions
select relname, reloptions from pg_class where oid in (
  't1'::regclass, 't2'::regclass, 't3'::regclass, 't4'::regclass,
  't5'::regclass, 't6'::regclass, 't7'::regclass, 't8'::regclass,
  't9'::regclass,
  't9_1_prt_p2024'::regclass,
  't9_1_prt_p2024_2_prt_north'::regclass, 't9_1_prt_p2024_2_prt_south'::regclass,
  't9_1_prt_p2025'::regclass,
  't9_1_prt_p2025_2_prt_north'::regclass, 't9_1_prt_p2025_2_prt_south'::regclass,
  't9_1_prt_p2026'::regclass,
  't9_1_prt_p2026_2_prt_north'::regclass, 't9_1_prt_p2026_2_prt_south'::regclass,
  't9_1_prt_p2027'::regclass,
  't9_1_prt_p2027_2_prt_north'::regclass, 't9_1_prt_p2027_2_prt_south'::regclass,
  't9_1_prt_p2028'::regclass,
  't9_1_prt_p2028_2_prt_north'::regclass, 't9_1_prt_p2028_2_prt_south'::regclass);

-- It is not permitted to change offloaded
alter table t7 set (offloaded=false);

-- Clean up
drop table t1, t2, t3, t4, t5, t6, t7, t8, t9;


--
-- AOCO

-- offloaded is not a column specific option
create table t(i int encoding(offloaded)) with(appendonly, orientation=column) distributed by(i);

-- Invalid value
create table t(i int) with(appendonly, orientation=column, offloaded=invalid_value) distributed by(i);

-- Set offloaded to true
create table t1(i int) with(appendonly, orientation=column, offloaded) distributed by(i);
create table t2(i int) with(appendonly, orientation=column, offloaded=true) distributed by(i);
create table t3(i int) with(appendonly, orientation=column, offloaded=1) distributed by(i);
create table t4(i int) with(appendonly, orientation=column, OFFLOADED=TRUE) distributed by(i);

-- Set offloaded to false
create table t5(i int) with(appendonly, orientation=column, OFFLOADED=0) distributed by(i);
create table t6(i int) with(appendonly, orientation=column, offloaded=false) distributed by(i);

-- We can set offloaded by default
set gp_default_storage_options='appendonly=false,blocksize=32768,compresstype=none,checksum=true,orientation=row,offloaded=true';
show gp_default_storage_options;

-- When offloaded is set by default it affects AO tables, but not heap ones
create table t7(i int) with(appendonly, orientation=column) distributed by(i);
reset gp_default_storage_options;

-- Partitions and subpartitions are offloaded when the table is created as offloaded
create table t9(id bigint, d date, r char(1), s numeric(18,2))
with(appendonly, orientation=column, offloaded)
distributed by (id)
partition by range (d)
subpartition by list (r)
subpartition template (
  subpartition north values ('N'),
  subpartition south values ('S')
)
(
  partition p2024 start (date '2024-01-01') end (date '2025-01-01'),
  partition p2025 start (date '2025-01-01') end (date '2026-01-01'),
  partition p2026 start (date '2026-01-01') end (date '2027-01-01')
);

-- Added partitions and subpartitions are not offloaded by default
alter table t9
add partition p2027 start (date '2027-01-01') end (date '2028-01-01');

-- We can add offloaded partitions
alter table t9
add partition p2028 start (date '2028-01-01') end (date '2029-01-01')
with(appendonly, orientation=column, offloaded);

-- Check reloptions
select relname, reloptions from pg_class where oid in (
  't1'::regclass, 't2'::regclass, 't3'::regclass, 't4'::regclass,
  't5'::regclass, 't6'::regclass, 't7'::regclass,
  't9'::regclass,
  't9_1_prt_p2024'::regclass,
  't9_1_prt_p2024_2_prt_north'::regclass, 't9_1_prt_p2024_2_prt_south'::regclass,
  't9_1_prt_p2025'::regclass,
  't9_1_prt_p2025_2_prt_north'::regclass, 't9_1_prt_p2025_2_prt_south'::regclass,
  't9_1_prt_p2026'::regclass,
  't9_1_prt_p2026_2_prt_north'::regclass, 't9_1_prt_p2026_2_prt_south'::regclass,
  't9_1_prt_p2027'::regclass,
  't9_1_prt_p2027_2_prt_north'::regclass, 't9_1_prt_p2027_2_prt_south'::regclass,
  't9_1_prt_p2028'::regclass,
  't9_1_prt_p2028_2_prt_north'::regclass, 't9_1_prt_p2028_2_prt_south'::regclass);

-- It is not permitted to change offloaded
alter table t7 set (offloaded=false);

-- Clean up
drop table t1, t2, t3, t4, t5, t6, t7, t9;
