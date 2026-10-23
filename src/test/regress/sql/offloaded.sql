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

-- Check reloptions
select relname, reloptions from pg_class where relname in ('t1', 't2', 't3', 't4', 't5', 't6', 't7', 't8');

-- It is not permitted to change offloaded
alter table t7 set (offloaded=false);

-- Clean up
drop table t1, t2, t3, t4, t5, t6, t7, t8;


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

-- Check reloptions
select relname, reloptions from pg_class where relname in ('t1', 't2', 't3', 't4', 't5', 't6', 't7');

-- It is not permitted to change offloaded
alter table t7 set (offloaded=false);

-- Clean up
drop table t1, t2, t3, t4, t5, t6, t7;

