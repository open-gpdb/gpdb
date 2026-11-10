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

--
-- RM_APPEND_ONLY_ID WAL records must not be written for offloaded tables.
--

-- WAL position, timeline, relfilenode and wal_level on every segment
create function wal_seg_state(rel regclass)
returns table (segid int, start_lsn text, tli int, relfn oid, wal_level text)
language plpgsql execute on all segments as $$
declare
    l pg_lsn := pg_current_xlog_location();
begin
    segid := gp_execution_segment();
    start_lsn := l::text;
    tli := ('x' || substr((pg_xlogfile_name_offset(l)).file_name, 1, 8))::bit(32)::int;
    wal_level := current_setting('wal_level');
    select c.relfilenode into relfn from pg_class c where c.oid = rel;
    return next;
end;
$$;

select datadir as seg0_datadir from gp_segment_configuration where content = 0 and role = 'p' \gset
\setenv SEG0_DATADIR :seg0_datadir

-- Preconditions: WAL is written, pg_xlogdump knows AO records, segment WAL is reachable
select wal_level <> 'minimal' as wal_enabled from wal_seg_state('pg_class') where segid = 0;
\! pg_xlogdump -r list 2>/dev/null | grep -qi appendonly && echo rmgr_found || echo rmgr_missing
\! test -d "$SEG0_DATADIR/pg_xlog" && echo wal_dir_found || echo wal_dir_missing

create table wal_ao     (i int) with (appendonly) distributed by (i);
create table wal_ao_off (i int) with (appendonly, offloaded) distributed by (i);
create table wal_co     (i int) with (appendonly, orientation=column) distributed by (i);
create table wal_co_off (i int) with (appendonly, orientation=column, offloaded) distributed by (i);

-- INSERT, AO row (appendonly_insert_init)
select start_lsn, tli, relfn from wal_seg_state('wal_ao') where segid = 0 \gset
\setenv START_LSN :start_lsn
\setenv TLI :tli
\setenv RELFN :relfn
insert into wal_ao select generate_series(1, 1000);
\! pg_xlogdump -p "$SEG0_DATADIR" -t "$TLI" -s "$START_LSN" 2>/dev/null | grep -iE 'rmgr: *append' | grep -qw "$RELFN" && echo present || echo absent

select start_lsn, tli, relfn from wal_seg_state('wal_ao_off') where segid = 0 \gset
\setenv START_LSN :start_lsn
\setenv TLI :tli
\setenv RELFN :relfn
insert into wal_ao_off select generate_series(1, 1000);
\! pg_xlogdump -p "$SEG0_DATADIR" -t "$TLI" -s "$START_LSN" 2>/dev/null | grep -iE 'rmgr: *append' | grep -qw "$RELFN" && echo present || echo absent

-- INSERT, AO column (open_ds_write)
select start_lsn, tli, relfn from wal_seg_state('wal_co') where segid = 0 \gset
\setenv START_LSN :start_lsn
\setenv TLI :tli
\setenv RELFN :relfn
insert into wal_co select generate_series(1, 1000);
\! pg_xlogdump -p "$SEG0_DATADIR" -t "$TLI" -s "$START_LSN" 2>/dev/null | grep -iE 'rmgr: *append' | grep -qw "$RELFN" && echo present || echo absent

select start_lsn, tli, relfn from wal_seg_state('wal_co_off') where segid = 0 \gset
\setenv START_LSN :start_lsn
\setenv TLI :tli
\setenv RELFN :relfn
insert into wal_co_off select generate_series(1, 1000);
\! pg_xlogdump -p "$SEG0_DATADIR" -t "$TLI" -s "$START_LSN" 2>/dev/null | grep -iE 'rmgr: *append' | grep -qw "$RELFN" && echo present || echo absent

-- ADD COLUMN, AO column (aocs_addcol_init)
select start_lsn, tli, relfn from wal_seg_state('wal_co') where segid = 0 \gset
\setenv START_LSN :start_lsn
\setenv TLI :tli
\setenv RELFN :relfn
alter table wal_co add column j int default 42;
\! pg_xlogdump -p "$SEG0_DATADIR" -t "$TLI" -s "$START_LSN" 2>/dev/null | grep -iE 'rmgr: *append' | grep -qw "$RELFN" && echo present || echo absent

select start_lsn, tli, relfn from wal_seg_state('wal_co_off') where segid = 0 \gset
\setenv START_LSN :start_lsn
\setenv TLI :tli
\setenv RELFN :relfn
alter table wal_co_off add column j int default 42;
\! pg_xlogdump -p "$SEG0_DATADIR" -t "$TLI" -s "$START_LSN" 2>/dev/null | grep -iE 'rmgr: *append' | grep -qw "$RELFN" && echo present || echo absent

-- VACUUM: compaction after DELETE (appendonly_insert_init) and truncate to EOF after abort (TruncateAOSegmentFile)
delete from wal_ao where i <= 500;
begin; insert into wal_ao select generate_series(1, 1000); abort;
select start_lsn, tli, relfn from wal_seg_state('wal_ao') where segid = 0 \gset
\setenv START_LSN :start_lsn
\setenv TLI :tli
\setenv RELFN :relfn
vacuum wal_ao;
\! pg_xlogdump -p "$SEG0_DATADIR" -t "$TLI" -s "$START_LSN" 2>/dev/null | grep -iE 'rmgr: *append' | grep -qw "$RELFN" && echo present || echo absent

delete from wal_ao_off where i <= 500;
begin; insert into wal_ao_off select generate_series(1, 1000); abort;
select start_lsn, tli, relfn from wal_seg_state('wal_ao_off') where segid = 0 \gset
\setenv START_LSN :start_lsn
\setenv TLI :tli
\setenv RELFN :relfn
vacuum wal_ao_off;
\! pg_xlogdump -p "$SEG0_DATADIR" -t "$TLI" -s "$START_LSN" 2>/dev/null | grep -iE 'rmgr: *append' | grep -qw "$RELFN" && echo present || echo absent

delete from wal_co where i <= 500;
begin; insert into wal_co select generate_series(1, 1000); abort;
select start_lsn, tli, relfn from wal_seg_state('wal_co') where segid = 0 \gset
\setenv START_LSN :start_lsn
\setenv TLI :tli
\setenv RELFN :relfn
vacuum wal_co;
\! pg_xlogdump -p "$SEG0_DATADIR" -t "$TLI" -s "$START_LSN" 2>/dev/null | grep -iE 'rmgr: *append' | grep -qw "$RELFN" && echo present || echo absent

delete from wal_co_off where i <= 500;
begin; insert into wal_co_off select generate_series(1, 1000); abort;
select start_lsn, tli, relfn from wal_seg_state('wal_co_off') where segid = 0 \gset
\setenv START_LSN :start_lsn
\setenv TLI :tli
\setenv RELFN :relfn
vacuum wal_co_off;
\! pg_xlogdump -p "$SEG0_DATADIR" -t "$TLI" -s "$START_LSN" 2>/dev/null | grep -iE 'rmgr: *append' | grep -qw "$RELFN" && echo present || echo absent

-- Partitions: offloaded leaf writes nothing, added non-offloaded partition writes records
create table wal_part (i int, d date) with (appendonly, offloaded) distributed by (i)
partition by range (d) (partition p_off start (date '2024-01-01') end (date '2025-01-01'));
alter table wal_part add partition p_norm start (date '2025-01-01') end (date '2026-01-01') with (appendonly);

select start_lsn, tli, relfn from wal_seg_state('wal_part_1_prt_p_off') where segid = 0 \gset
\setenv START_LSN :start_lsn
\setenv TLI :tli
\setenv RELFN :relfn
insert into wal_part select g, date '2024-06-01' from generate_series(1, 1000) g;
\! pg_xlogdump -p "$SEG0_DATADIR" -t "$TLI" -s "$START_LSN" 2>/dev/null | grep -iE 'rmgr: *append' | grep -qw "$RELFN" && echo present || echo absent

select start_lsn, tli, relfn from wal_seg_state('wal_part_1_prt_p_norm') where segid = 0 \gset
\setenv START_LSN :start_lsn
\setenv TLI :tli
\setenv RELFN :relfn
insert into wal_part select g, date '2025-06-01' from generate_series(1, 1000) g;
\! pg_xlogdump -p "$SEG0_DATADIR" -t "$TLI" -s "$START_LSN" 2>/dev/null | grep -iE 'rmgr: *append' | grep -qw "$RELFN" && echo present || echo absent

-- Clean up
drop table wal_ao, wal_ao_off, wal_co, wal_co_off, wal_part;
drop function wal_seg_state(regclass);
