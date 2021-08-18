# $Id$
#
# SYNOPSIS
#
#   AX_PBS([ACTION-IF-FOUND[, ACTION-IF-NOT-FOUND]])
#
# DESCRIPTION
#
#   Check for PBS libraries and headers.
#
#   This macro calls::
#
#     AC_SUBST(PBS_INCLUDES)
#     AC_SUBST(PBS_LIBS)
#     AC_SUBST(PBS_LDFLAGS)
#
# LAST MODIFICATION
#
#   2008-06-13
#
# LICENSE
#
#   Written by Łukasz Cieśnik <lukasz.ciesnik@gmail.com>
#   and placed under Public Domain. 
#   
#   Further contribution: Mariusz Mamonski <mamonski@man.poznan.pl> 
#

AC_DEFUN([AX_PBS],[
AC_ARG_WITH([pbs], [AS_HELP_STRING([--with-pbs=<pbs-prefix>],[Path to existing PBS installation root])])
AC_ARG_ENABLE([disable-pbs-log], [AS_HELP_STRING([--disable-pbs-log],[Do not use liblog while linking with PBS Professional])])
		
AC_SUBST(PBS_INCLUDES)
AC_SUBST(PBS_LIBS)
AC_SUBST(PBS_LDFLAGS)

if test x"$with_pbs" != x; then
	PBS_HOME=$with_pbs
	PBS_INCLUDES="-I${with_pbs}/include "
	PBS_LDFLAGS="-L${with_pbs}/lib "
else
	T1=`which pbsnodes`
	if test x"$T1" != x; then
		T2=`dirname $T1`
		PBS_HOME=`dirname $T2`
		PBS_INCLUDES="-I${PBS_HOME}/include "
		PBS_LDFLAGS="-L${PBS_HOME}/lib "
	fi
fi


PBS_INCLUDES="$PBS_INCLUDES -I${PBS_HOME}/include/torque "
LDFLAGS_save="$LDFLAGS"
CPPFLAGS_save="$CPPFLAGS"
LDFLAGS="$LDFLAGS $PBS_LDFLAGS"
CPPFLAGS="$CPPFLAGS $PBS_INCLUDES"

ax_pbs_ok="no"

AH_TEMPLATE([PBS_PROFESSIONAL], [compiling against PBS Professional])
AH_TEMPLATE([PBS_PROFESSIONAL_NO_LOG], [Do not use liblog while linking with PBS Professional])
AH_TEMPLATE([HAVE_PBS_SUBMIT_HASH], [Torque 4 pbs_submit_hash function found])

if test x"$enable_pbs_log" != "xno"; then
	ax_pbs_lib_log=" -llog"
else
	ax_pbs_lib_log=""
	AC_DEFINE(PBS_PROFESSIONAL_NO_LOG,[1])
fi

if test x"$ax_pbs_ok" = xno; then
	ax_pbs_ok="yes"
	AC_CHECK_LIB([pbs], [pbs_submit], [:], [ax_pbs_ok="no"])
	AC_CHECK_LIB([log], [pbse_to_txt], [:], [ax_pbs_ok="no"])
	if test x"$ax_pbs_ok" = xyes; then
		ax_pbs_libs="-lpbs $ax_pbs_lib_log"
	fi
fi

AS_UNSET([ac_cv_lib_pbs_pbs_submit])
AS_UNSET([ac_cv_lib_log_pbse_to_txt])

if test x"$ax_pbs_ok" = xno; then
	ax_pbs_ok="yes"
	AC_CHECK_LIB([pbs], [pbs_submit], [:], [ax_pbs_ok="no"], [-lssl -lcrypto])
	AC_CHECK_LIB([log], [pbse_to_txt], [:], [ax_pbs_ok="no"], [-lssl -lcrypto] )
	if test x"$ax_pbs_ok" = xyes; then
		ax_pbs_libs="-lpbs $ax_pbs_lib_log -lssl -lcrypto"
	fi
fi

ax_pbs_submit_hash="no"

if test x"$ax_pbs_ok" = xno; then
 	ax_pbs_ok="yes"
 	AC_CHECK_LIB([torque], [pbs_submit], [:], [ax_pbs_ok="no"])
 	AC_CHECK_LIB([torque], [pbse_to_txt], [:], [ax_pbs_ok="no"])
	AC_CHECK_LIB([torque], [pbs_submit_hash], [AC_DEFINE(HAVE_PBS_SUBMIT_HASH) ax_pbs_submit_hash="yes"], [:])


 	if test x"$ax_pbs_ok" = xyes; then
 		ax_pbs_libs="-ltorque"
 	fi

	AM_CONDITIONAL([PBS_PROFESSIONAL], [false])
else
	AC_DEFINE(PBS_PROFESSIONAL,[1])
	AM_CONDITIONAL([PBS_PROFESSIONAL], [true])
fi

AM_CONDITIONAL([TORQUE4], [test x$ax_pbs_submit_hash = xyes])

if test x"$ax_pbs_ok" = xyes; then
	AC_CHECK_HEADERS([pbs_ifl.h pbs_error.h],[:],[ax_pbs_ok="no"])
fi

LDFLAGS="$LDFLAGS_save"
CPPFLAGS="$CPPFLAGS_save"

if test x"$ax_pbs_ok" = xyes; then
	PBS_LIBS="$ax_pbs_libs"
	ifelse($1, , :, $1) 
else
	ifelse($2, , :, $2) 
fi
])
