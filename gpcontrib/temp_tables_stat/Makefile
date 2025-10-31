# gpcontrib/temp_tables_stat/Makefile

MODULE_big = temp_tables_stat
OBJS = temp_tables_stat.o $(WIN32RES)

EXTENSION = temp_tables_stat
PGFILEDESC = "Extension for getting statistics on temporary tables"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/temp_tables_stat
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
