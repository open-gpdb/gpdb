# gpcontrib/gp_aux_catalog/Makefile

MODULE_big = gp_aux_catalog
OBJS = gp_aux_catalog.o $(WIN32RES)

EXTENSION = gp_aux_catalog
DATA = gp_aux_catalog--1.0.sql gp_aux_catalog--1.0--1.1.sql
PGFILEDESC = "gp_aux_catalog - An auxiliar catalog extension for Greenplum"

REGRESS = pg_event_trigger_ddl_commands

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gp_aux_catalog
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

