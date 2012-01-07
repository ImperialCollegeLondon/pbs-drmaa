/* $Id: example.c 49 2011-12-08 07:36:40Z mmamonski $ */
/*
 *  DRMAA library for Torque/PBS
 *  Copyright (C) 2006-2007
 *
 *  FedStage Systems <http://www.fedstage.com/>,
 *  Poznan Supercomputing and Networking Center <http://www.man.poznan.pl/>,
 *  and the OpenDSP project <http://sourceforge.net/projects/opendsp/>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
/**
 * @file test.c
 * DRMAA library test / usage example.
 */

#include <drmaa.h>

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <errno.h>


#define ERR_LEN      DRMAA_ERROR_STRING_BUFFER
#define JOB_ID_LEN   256

#define errno_error( func ) do{                      \
	 fprintf( stderr, "%s:%d: %s: %s\n",               \
			 __FILE__, __LINE__, func, strerror(errno) );  \
	 exit( 1 );                                        \
 } while(0)

#define drmaa_error( func ) do{                      \
		fprintf( stderr, "%s:%d: %s: %s\n> %s\n",        \
				__FILE__, __LINE__, func,                    \
				drmaa_strerror(rc), err_msg );               \
		exit(1);                                         \
	}while(0)

void
test_1(void);

drmaa_job_template_t *
construct_job( char *argv[], char *fds[3], const char *cwd );

void
time_fmt( time_t t, char *buf, size_t buflen );


int
main( int argc, char *argv[] )
{
	char err_msg[ ERR_LEN ] = "";
	char job_id[128] = "";
	drmaa_attr_values_t *rusage;
	int rc, stat;


	rc = drmaa_init( NULL, err_msg, ERR_LEN );
	if( rc )  
		drmaa_error("drmaa_init");

	rc = drmaa_wait( argv[1], job_id, sizeof(job_id), &stat, DRMAA_TIMEOUT_WAIT_FOREVER, &rusage, err_msg, ERR_LEN );
	if( rc )
		drmaa_error( "drmaa_job_wait" );

	rc = drmaa_exit( err_msg, ERR_LEN );
	if( rc )  
		drmaa_error("drmaa_exit");

	printf( "stat=%d\n", stat );

	return 0;
}

