# gpcontrib/ast_log/Makefile

MODULE_big = ast_log
OBJS = ast_log.o

EXTENSION = ast_log
DATA = ast_log--1.0.sql
PGFILEDESC = "ast_log - AST logging extension for Greenplum"

REGRESS = ast_log

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/ast_log
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
