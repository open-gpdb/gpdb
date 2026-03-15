#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for motion (CDB2)
#
#-------------------------------------------------------------------------


subdir = src/backend/cdb/motion
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = cdbmotion.o tupchunklist.o tupser.o  \
	ic_common.o ic_tcp.o ic_udpifc.o htupfifo.o tupleremap.o

ifeq ($(enable_ic_proxy),yes)
# server
OBJS += ic_proxy_bgworker.o
OBJS += ic_proxy_main.o
OBJS += ic_proxy_client.o
OBJS += ic_proxy_peer.o
OBJS += ic_proxy_router.o

# backend
OBJS += ic_proxy_backend.o

# utils
OBJS += ic_proxy_addr.o
OBJS += ic_proxy_key.o
OBJS += ic_proxy_packet.o
OBJS += ic_proxy_pkt_cache.o
OBJS += ic_proxy_iobuf.o
endif  # enable_ic_proxy

ifdef USE_VPP
OBJS += gpvpp_shim.o
override CPPFLAGS += -DUSE_VPP
# VPP VPPCOM headers and library.
# Set VPP_PREFIX if VPP is not installed in /usr.
VPP_PREFIX ?= /usr
override CPPFLAGS += -I$(VPP_PREFIX)/include
override LDFLAGS += -L$(VPP_PREFIX)/lib -lvppcom
endif  # USE_VPP

include $(top_srcdir)/src/backend/common.mk
