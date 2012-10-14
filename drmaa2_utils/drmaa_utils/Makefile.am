# $Id: Makefile.am 18 2011-10-12 12:21:55Z mmamonski $
#
# PSNC DRMAA 2.0 utilities library
# Copyright (C) 2012  Poznan Supercomputing and Networking Center
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



noinst_LTLIBRARIES = libdrmaa_utils.la
COMMON_SOURCES = \
 common.h \
 compat.h compat.c \
 conf_impl.h conf_tab.c conf_tab.h conf.c conf.h \
 drmaa2.h \
 environ.h environ.c \
 exception.c exception.h \
 util.h fsd_util.c \
 iter.h iter.c \
 logging.h logging.c \
 lookup3.c lookup.h \
 thread.h thread.c \
 xmalloc.h xmalloc.c
# job.h fsd_job.c \
# session.h fsd_session.c \
# drmaa_util.c drmaa_util.h
 
 
 


libdrmaa_utils_la_SOURCES = $(COMMON_SOURCES) \
 drmaa_base.c drmaa_base.h


libdrmaa_utils_la_LDFLAGS = -static
libdrmaa_utils_la_CPPFLAGS = -fPIC


include_HEADERS = drmaa2.h

EXTRA_DIST = conf_tab.y

if DEVELOPER_MODE
CLEANFILES = $(BUILT_SOURCES)
endif




