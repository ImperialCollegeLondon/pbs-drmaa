# $Id: Makefile.am 3 2009-10-11 18:40:05Z mamonski $

CLEANFILES = *.core *.pyc *.pyo

AM_CPPFLAGS = -I$(top_srcdir)/drmaa2_utils/drmaa_utils

LDADD = $(top_builddir)/pbs_drmaa/libdrmaa.la

TEST_NAMES = \
  drmaa_example1


TESTS = $(TEST_NAMES)
check_PROGRAMS = $(TEST_NAMES)


