MODULE_big      = hypodist
OBJS            = ./src/hypodist.o
EXTENSION       = hypodist
EXTVERSION      = 1.0
DATA            = $(wildcard sql/*--*.sql)
REGRESS         = hypodist
REGRESS_OPTS    = --inputdir=test/
PGFILEDESC      = "hypodist - an extension use hypothetical redistribute keys for gp tables"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/hypodict
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

