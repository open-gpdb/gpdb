-- start_ignore
create extension if not exists gp_aux_catalog;
drop event trigger if exists test_pg_event_trigger_ddl_commands;
drop function if exists test_pg_event_trigger_ddl_commands();
drop type if exists color;
drop trigger if exists t_trigger on t;
drop function if exists t_trigger();
drop table if exists t;
drop external table if exists ext_t1;
drop text search configuration if exists tsc_pg_event_trigger_ddl_commands;
drop collation if exists test0;
drop protocol if exists demoprot;
drop function if exists read_from_file();
drop function if exists write_to_file();
drop domain if exists con;
-- end_ignore


-- Create an event trigger to output information about DDL queries
create or replace function test_pg_event_trigger_ddl_commands()
returns event_trigger as
$$
declare
	r record;
begin
	for r in select * from pg_event_trigger_ddl_commands()
	loop
		raise notice 'classid = %, command_tag = %, object_type = %, schema_name = %, object_identity = %, in_extension = %',
			r.classid, r.command_tag, r.object_type, r.schema_name, r.object_identity, r.in_extension;
	end loop;
end;
$$ language plpgsql;

create event trigger test_pg_event_trigger_ddl_commands
on ddl_command_end
execute procedure test_pg_event_trigger_ddl_commands();


-- The following queries should work without errors when the event
-- trigger calls pg_event_trigger_ddl_commands()

-- Check T_IndexStmt
create table t(i int primary key) distributed by (i);


-- Check T_CommentStmt
comment on column t.i is 'comment for column i';


-- Check T_AlterExtensionContentsStmt
alter extension gp_aux_catalog add table t;
alter extension gp_aux_catalog drop table t;


-- Check T_CreateTrigStmt
create or replace function t_trigger()
returns trigger as
$$
begin
	raise notice 't_trigger';
	return null;
end;
$$ language plpgsql;

create trigger t_trigger after insert on t for each statement
execute procedure t_trigger();

drop trigger t_trigger on t;
drop function t_trigger();
drop table t;


-- Check T_AlterTypeStmt
create type color as enum ('red', 'green', 'blue');
alter type color set default encoding (compresstype=zlib);
drop type color;


-- Check T_CreateExternalStmt
create external table ext_t1 (a int, b int)
location ('file:///tmp/test.txt') format 'text';

drop external table ext_t1;


-- Check T_DefineStmt, OBJECT_TSCONFIGURATION
create text search configuration tsc_pg_event_trigger_ddl_commands (
	copy = pg_catalog.english
);
drop text search configuration tsc_pg_event_trigger_ddl_commands;


-- Check T_DefineStmt, OBJECT_COLLATION
create collation test0 from "C";
drop collation test0;


-- Check T_DefineStmt, OBJECT_EXTPROTOCOL
create or replace function write_to_file()
returns int as
'$libdir/gpextprotocol.so', 'demoprot_export' language C stable;

create or replace function read_from_file()
returns int as
'$libdir/gpextprotocol.so', 'demoprot_import' language C stable;

create protocol demoprot (
	readfunc  = read_from_file,
	writefunc = write_to_file
);

drop protocol demoprot;
drop function read_from_file();
drop function write_to_file();


-- Check T_AlterDomainStmt
create domain con as int;
alter domain con add constraint t check (value < 34);
drop domain con;


-- Cleanup
drop event trigger test_pg_event_trigger_ddl_commands;
drop function test_pg_event_trigger_ddl_commands();
