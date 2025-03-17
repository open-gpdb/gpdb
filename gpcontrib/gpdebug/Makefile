# gpcontrib/gpdebug/Makefile

MODULE_big = gpdebug
OBJS = gpdebug.o $(WIN32RES)

EXTENSION = gpdebug
DATA = gpdebug--1.0.sql
PGFILEDESC = "gpdebug - An debug logging for PostgreSQL"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
