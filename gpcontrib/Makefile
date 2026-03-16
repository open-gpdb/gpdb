# gpcontrib/Makefile

subdir = gpcontrib
top_builddir = ..
include $(top_builddir)/src/Makefile.global

SUBDIRS = \
		gp_array_agg				\
		gp_aux_catalog				\
		gp_check_functions			\
		gp_distribution_policy 		\
		gp_error_handling			\
		gp_interconnect_stats		\
		gp_internal_tools			\
		gp_legacy_string_agg		\
		gp_parallel_retrieve_cursor	\
		gp_parser					\
		gp_percentile_agg			\
		gp_pitr						\
		gp_sparse_vector 			\
		gp_subtransaction_overflow	\
		jsonlog-gpdb				\
		temp_tables_stat

ifeq ($(enable_debug_extensions),yes)
SUBDIRS += gp_debug_numsegments gp_inject_fault gp_replica_check
else
ALWAYS_SUBDIRS += gp_debug_numsegments gp_inject_fault gp_replica_check
endif

ifeq "$(with_yagp_hooks_collector)" "yes"
SUBDIRS += yagp_hooks_collector
else
ALWAYS_SUBDIRS += yagp_hooks_collector
endif

ifeq "$(with_zstd)" "yes"
SUBDIRS += zstd
else
ALWAYS_SUBDIRS += zstd
endif

ifeq "$(with_quicklz)" "yes"
SUBDIRS += quicklz
else
ALWAYS_SUBDIRS += quicklz
endif

ifeq "$(enable_yezzey)" "yes"
SUBDIRS += yezzey
else
ALWAYS_SUBDIRS += yezzey
endif

ifeq "$(enable_mapreduce)" "yes"
SUBDIRS += gpmapreduce
else
ALWAYS_SUBDIRS += gpmapreduce
endif

ifeq "$(enable_orafce)" "yes"
SUBDIRS += orafce
else
ALWAYS_SUBDIRS += orafce
endif

ifeq "$(enable_sr_plan)" "yes"
SUBDIRS += sr_plan
else
ALWAYS_SUBDIRS += sr_plan
endif

ifeq "$(enable_pgaudit)" "yes"
SUBDIRS += pgaudit
else
ALWAYS_SUBDIRS += pgaudit
endif

ifeq "$(enable_gpcloud)" "yes"
SUBDIRS += gpcloud gpcloud/bin/gpcheckcloud
else
ALWAYS_SUBDIRS += gpcloud gpcloud/bin/gpcheckcloud
endif


$(call recurse,all install clean distclean)
$(call recurse,installcheck,$(filter-out yezzey, $(SUBDIRS)))
$(recurse_always)
