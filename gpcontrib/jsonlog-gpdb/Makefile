OBJS = jsonlog.o

MODULE_big = jsonlog-gpdb


ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/jsonlog-gpdb
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

MODULES = jsonlog-gpdb
