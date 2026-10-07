# contrib/custom_scan_test/Makefile

MODULES = custom_scan_test

EXTENSION = custom_scan_test

REGRESS = custom_scan_test

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/custom_scan_test
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
