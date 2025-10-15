# protect the default target for this file from the targets in Makefile.global
default: all

top_builddir = ..
include $(top_builddir)/src/Makefile.global

.PHONY: all install clean distclean gpcloud mapreduce pgaudit sr_plan

#
# targets built via top_builddir/GNUmakefile
#
recurse_targets = ""

ifeq "$(enable_debug_extensions)" "yes"
	recurse_targets = gp_sparse_vector \
               gp_distribution_policy \
               gp_parallel_retrieve_cursor \
               gp_internal_tools \
               gp_pitr \
               gp_debug_numsegments \
               gp_inject_fault \
               gp_replica_check \
               gp_legacy_string_agg \
               gp_array_agg \
               gp_percentile_agg \
               gp_error_handling \
               gp_subtransaction_overflow \
               gp_check_functions \
               gp_aux_catalog \
			   gp_interconnect_stats
else
	recurse_targets = gp_sparse_vector \
               gp_distribution_policy \
               gp_parallel_retrieve_cursor \
               gp_internal_tools \
               gp_pitr \
               gp_parser \
               jsonlog-gpdb \
               gp_legacy_string_agg \
               gp_array_agg \
               gp_percentile_agg \
               gp_error_handling \
               gp_subtransaction_overflow \
               gp_check_functions \
               gp_aux_catalog \
			   gp_interconnect_stats
endif

ifeq "$(with_zstd)" "yes"
	recurse_targets += zstd
endif
ifeq "$(with_quicklz)" "yes"
	recurse_targets += quicklz
endif
$(call recurse,all install clean distclean, $(recurse_targets))

all: gpcloud mapreduce orafce yezzey sr_plan pgaudit

yezzey:
	@if [ "$(enable_yezzey)" = "yes" ]; then \
		$(MAKE) -C yezzey
	fi

gpcloud:
	@if [ "$(enable_gpcloud)" = "yes" ]; then \
		$(MAKE) -C gpcloud && \
		$(MAKE) -C gpcloud/bin/gpcheckcloud; \
	fi

mapreduce:
	@if [ "$(enable_mapreduce)" = "yes" ]; then \
		$(MAKE) -C gpmapreduce; \
	fi

orafce:
	@if [ "$(enable_orafce)" = "yes" ]; then \
		$(MAKE) -C orafce NO_PGXS=true; \
	fi

sr_plan:
	@if [ "$(enable_sr_plan)" = "yes" ]; then \
  		$(MAKE) -C sr_plan NO_PGXS=true; \
 	fi

pgaudit:
	@if [ "$(enable_pgaudit)" = "yes" ]; then \
  		$(MAKE) -C pgaudit NO_PGXS=true; \
 	fi


install:
	@if [ "$(enable_gpcloud)" = "yes" ]; then \
		$(MAKE) -C gpcloud install && \
		$(MAKE) -C gpcloud/bin/gpcheckcloud install; \
	fi
	@if [ "$(enable_yezzey)" = "yes" ]; then \
		$(MAKE) -C yezzey install; \
	fi
	@if [ "$(enable_mapreduce)" = "yes" ]; then \
		$(MAKE) -C gpmapreduce install; \
	fi
	@if [ "$(enable_orafce)" = "yes" ]; then \
		$(MAKE) -C orafce NO_PGXS=true install; \
	fi
	@if [ "$(enable_pgaudit)" = "yes" ]; then \
                $(MAKE) -C pgaudit NO_PGXS=true install; \
        fi
	@if [ "$(enable_sr_plan)" = "yes" ]; then \
                $(MAKE) -C sr_plan NO_PGXS=true install; \
        fi

clean:
	if [ "$(enable_mapreduce)" = "yes" ]; then $(MAKE) -C gpmapreduce clean; fi
	if [ "$(enable_gpcloud)" = "yes" ]; then $(MAKE) -C gpcloud cleanall; fi
	if [ "$(enable_yezzey)" = "yes" ]; then $(MAKE) -C yezzey cleanall; fi
	if [ "${enable_orafce}" = "yes" ]; then $(MAKE) -C orafce NO_PGXS=true clean; fi
	if [ "${enable_sr_plan}" = "yes" ]; then $(MAKE) -C sr_plan clean; fi
	if [ "${enable_pgaudit}" = "yes" ]; then $(MAKE) -C pgaudit clean; fi


distclean:
	if [ "$(enable_mapreduce)" = "yes" ]; then $(MAKE) -C gpmapreduce distclean; fi
	if [ "$(enable_gpcloud)" = "yes" ]; then $(MAKE) -C gpcloud distclean; fi
	if [ "$(enable_yezzey)" = "yes" ]; then $(MAKE) -C yezzey distclean; fi
	if [ "${enable_orafce}" = "yes" ]; then $(MAKE) -C orafce NO_PGXS=true distclean; fi

installcheck:
	$(MAKE) -C gp_internal_tools installcheck
	$(MAKE) -C gp_array_agg installcheck
	$(MAKE) -C gp_aux_catalog installcheck
	if [ "$(enable_mapreduce)" = "yes" ]; then \
		$(MAKE) -C gpmapreduce installcheck; \
	fi
	if [ "$(enable_orafce)" = "yes" ]; then $(MAKE) -C orafce installcheck; fi
	if [ "$(with_zstd)" = "yes" ]; then $(MAKE) -C zstd installcheck; fi
	if [ "$(with_quicklz)" = "yes" ]; then $(MAKE) -C quicklz installcheck; fi
	$(MAKE) -C gp_sparse_vector installcheck
	$(MAKE) -C gp_percentile_agg installcheck
	$(MAKE) -C gp_subtransaction_overflow installcheck
	$(MAKE) -C gp_check_functions installcheck
	if [ "${enable_pgaudit}" = "yes" ]; then $(MAKE) -C pgaudit installcheck; fi
