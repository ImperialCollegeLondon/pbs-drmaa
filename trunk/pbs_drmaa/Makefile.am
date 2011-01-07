# $Id: Makefile.am 2420 2009-06-24 14:40:21Z lukasz $
# 
# FedStage DRMAA for PBS Professional
# Copyright (C) 2006-2009  FedStage Systems
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

GPERF       = @GPERF@
GPERFFLAGS  = --readonly-tables
AM_CPPFLAGS = @PBS_INCLUDES@ -I$(top_srcdir)/drmaa_utils -fPIC

lib_LTLIBRARIES = libdrmaa.la

libdrmaa_la_SOURCES = \
 drmaa.c \
 job.c job.h \
 pbs_attrib.gperf pbs_attrib.h \
 session.c session.h \
 submit.c submit.h \
 util.c util.h
BUILT_SOURCES       = pbs_attrib.c
EXTRA_DIST          = pbs_attrib.c
dist_sysconf_DATA   = pbs_drmaa.conf.example
libdrmaa_la_LIBADD  = ../drmaa_utils/drmaa_utils/libdrmaa_utils.la @PBS_LIBS@
libdrmaa_la_LDFLAGS = @PBS_LDFLAGS@ -version-info @PBS_DRMAA_VERSION_INFO@


.gperf.c:
	$(GPERF) $(GPERFFLAGS) --output-file=$@ $<

if GCC
pbs_attrib.lo: CFLAGS += @GCC_W_NO_MISSING_FIELD_INITIALIZERS@
endif
