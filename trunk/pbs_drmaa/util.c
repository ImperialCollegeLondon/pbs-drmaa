/* $Id$ */
/*
 *  FedStage DRMAA for PBS Pro
 *  Copyright (C) 2006-2007  FedStage Systems
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * @file pbs_drmaa/util.c
 * PBS DRMAA utilities.
 */

#ifdef HAVE_CONFIG_H
#	include <config.h>
#endif

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>


#include <drmaa_utils/common.h>
#include <pbs_drmaa/util.h>
#include <pbs_error.h>
#include <pbs_ifl.h>

#ifndef lint
static char rcsid[]
#	ifdef __GNUC__
		__attribute__ ((unused))
#	endif
	= "$Id$";
#endif


void
pbsdrmaa_dump_attrl( const struct attrl *attribute_list, const char *prefix )
{
	const struct attrl *i;

	if( prefix == NULL )
		prefix = "";
	for( i = attribute_list;  i != NULL;  i = i->next )
		fsd_log_debug(( "\n %s %s%s%s=%s",
				prefix, i->name,
				i->resource ? "." : "",  i->resource ? i->resource : "",
				i->value
				));
}


void
pbsdrmaa_free_attrl( struct attrl *attr )
{
	while( attr != NULL )
	 {
		struct attrl *p = attr;
		attr = attr->next;
		fsd_free( p->name );
		fsd_free( p->value );
		fsd_free( p->resource );
		fsd_free( p );
	 }
}

struct attrl *
pbsdrmaa_add_attr( struct attrl *head, const char *name, const char *value)
{
	struct attrl *p = NULL;
	char *resource = NULL;

	fsd_malloc( p, struct attrl );
	memset( p, 0, sizeof(struct attrl) );

	resource = strchr( name, '.' );

	if( resource )
	 {
		p->name = fsd_strndup( name, resource - name );
		p->resource = fsd_strdup( resource+1 );
	 }
	else
	 {
		p->name = fsd_strdup( name );
	 }

	p->value = fsd_strdup(value);
	p->op = SET;

	fsd_log_debug(("set attr: %s = %s", name, value));

	if (head)
		p->next = head;
	else
		p->next = NULL;

	return p;
}


void
pbsdrmaa_exc_raise_pbs( const char *function )
{
	int _pbs_errno;
	int fsd_errno;
	const char *message = NULL;

	_pbs_errno = pbs_errno;

#ifndef PBS_PROFESSIONAL_NO_LOG
	message = pbse_to_txt( pbs_errno );
#else
	message = "PBS error";
#endif

	fsd_errno = pbsdrmaa_map_pbs_errno( _pbs_errno );
	fsd_log_error((
				"call to %s returned with error %d:%s mapped to %d:%s",
				function,
				_pbs_errno, message,
				fsd_errno, fsd_strerror(fsd_errno)
				));
	fsd_exc_raise_fmt( fsd_errno, " %s", function, message );
}


/** Maps PBS error code into DMRAA code. */
int
pbsdrmaa_map_pbs_errno( int _pbs_errno )
{
	fsd_log_enter(( "(pbs_errno=%d)", _pbs_errno ));
	switch( _pbs_errno )
	 {
		case PBSE_NONE:  /* no error */
			return FSD_ERRNO_SUCCESS;
		case PBSE_UNKJOBID:	 /* Unknown Job Identifier */
			return FSD_DRMAA_ERRNO_INVALID_JOB;
		case PBSE_NOATTR: /* Undefined Attribute */
		case PBSE_ATTRRO: /* attempt to set READ ONLY attribute */
		case PBSE_IVALREQ:  /* Invalid request */
		case PBSE_UNKREQ:  /* Unknown batch request */
			return FSD_ERRNO_INTERNAL_ERROR;
		case PBSE_PERM:  /* No permission */
		case PBSE_BADHOST:  /* access from host not allowed */
			return FSD_ERRNO_AUTHZ_FAILURE;
		case PBSE_JOBEXIST:  /* job already exists */
		case PBSE_SVRDOWN:  /* req rejected -server shutting down */
		case PBSE_EXECTHERE:  /* cannot execute there */
		case PBSE_NOSUP:  /* Feature/function not supported */
		case PBSE_EXCQRESC:  /* Job exceeds Queue resource limits */
		case PBSE_QUENODFLT:  /* No Default Queue Defined */
		case PBSE_NOTSNODE:  /* no time-shared nodes */
			return FSD_ERRNO_DENIED_BY_DRM;
		case PBSE_SYSTEM:  /* system error occurred */
		case PBSE_INTERNAL:  /* internal server error occurred */
		case PBSE_REGROUTE:  /* parent job of dependent in rte que */
		case PBSE_UNKSIG:  /* unknown signal name */
			return FSD_ERRNO_INTERNAL_ERROR;
		case PBSE_BADATVAL:  /* bad attribute value */
		case PBSE_BADATLST:  /* Bad attribute list structure */
		case PBSE_BADUSER:  /* Bad user - no password entry */
		case PBSE_BADGRP:  /* Bad Group specified */
		case PBSE_BADACCT:  /* Bad Account attribute value */
		case PBSE_UNKQUE:  /* Unknown queue name */
		case PBSE_UNKRESC:  /* Unknown resource */
		case PBSE_UNKNODEATR:  /* node-attribute not recognized */
		case PBSE_BADNDATVAL:  /* Bad node-attribute value */
		case PBSE_BADDEPEND:  /* Invalid dependency */
		case PBSE_DUPLIST:  /* Duplicate entry in List */
			return FSD_ERRNO_INVALID_VALUE;
		case PBSE_MODATRRUN:  /* Cannot modify attrib in run state */
		case PBSE_BADSTATE:  /* request invalid for job state */
		case PBSE_BADCRED:  /* Invalid Credential in request */
		case PBSE_EXPIRED:  /* Expired Credential in request */
		case PBSE_QUNOENB:  /* Queue not enabled */
			return FSD_ERRNO_INTERNAL_ERROR;
		case PBSE_QACESS:  /* No access permission for queue */
			return FSD_ERRNO_AUTHZ_FAILURE;
		case PBSE_HOPCOUNT:  /* Max hop count exceeded */
		case PBSE_QUEEXIST:  /* Queue already exists */
		case PBSE_ATTRTYPE:  /* incompatable queue attribute type */
			return FSD_ERRNO_INTERNAL_ERROR;
#		ifdef PBSE_QUEBUSY
		case PBSE_QUEBUSY:  /* Queue Busy (not empty) */
#		endif
		case PBSE_MAXQUED:  /* Max number of jobs in queue */
		case PBSE_NOCONNECTS:  /* No free connections */
		case PBSE_TOOMANY:  /* Too many submit retries */
		case PBSE_RESCUNAV:  /* Resources temporarily unavailable */
			return FSD_ERRNO_TRY_LATER;
		case 111:
		case PBSE_PROTOCOL:  /* Protocol (ASN.1) error */
		case PBSE_DISPROTO:  /* Bad DIS based Request Protocol */
			return FSD_ERRNO_DRM_COMMUNICATION_FAILURE;
#if 0
		case PBSE_QUENBIG:  /* Queue name too long */
		case PBSE_QUENOEN:  /* Cannot enable queue,needs add def */
		case PBSE_NOSERVER:  /* No server to connect to */
		case PBSE_NORERUN:  /* Job Not Rerunnable */
		case PBSE_ROUTEREJ:  /* Route rejected by all destinations */
		case PBSE_ROUTEEXPD:  /* Time in Route Queue Expired */
		case PBSE_MOMREJECT:  /* Request to MOM failed */
		case PBSE_BADSCRIPT:  /* (qsub) cannot access script file */
		case PBSE_STAGEIN:  /* Stage In of files failed */
		case PBSE_CKPBSY:  /* Checkpoint Busy, may be retries */
		case PBSE_EXLIMIT:  /* Limit exceeds allowable */
		case PBSE_ALRDYEXIT:  /* Job already in exit state */
		case PBSE_NOCOPYFILE:  /* Job files not copied */
		case PBSE_CLEANEDOUT:  /* unknown job id after clean init */
		case PBSE_NOSYNCMSTR:  /* No Master in Sync Set */
		case PBSE_SISREJECT:  /* sister rejected */
		case PBSE_SISCOMM:  /* sister could not communicate */
		case PBSE_CKPSHORT:  /* not all tasks could checkpoint */
		case PBSE_UNKNODE:  /* Named node is not in the list */
		case PBSE_NONODES:  /* Server has no node list */
		case PBSE_NODENBIG:  /* Node name is too big */
		case PBSE_NODEEXIST:  /* Node name already exists */
		case PBSE_MUTUALEX:  /* State values are mutually exclusive */
		case PBSE_GMODERR:  /* Error(s) during global modification of nodes */
		case PBSE_NORELYMOM:  /* could not contact Mom */
			return FSD_ERRNO_INTERNAL_ERROR;
#endif
		default:
			return FSD_ERRNO_INTERNAL_ERROR;
	 }
}


char *
pbsdrmaa_write_tmpfile( const char *content, size_t len )
{
	static const char *tmpfile_template = "/tmp/pbs_drmaa.XXXXXX";
	char *volatile name = NULL;
	volatile int fd = -1;

	fsd_log_enter(( "" ));

	TRY
	 {
		name = fsd_strdup( tmpfile_template );
		fd = mkstemp( name );
		if( fd < 0 )
			fsd_exc_raise_sys(0);

		if( fchmod(fd, 0600 ) != 0)
			fsd_exc_raise_sys(0);

		while( len > 0 )
		 {
			size_t written = write( fd, content, len );
			if( written != (size_t)-1 )
			 {
				content += written;
				len -= written;
			 }
			else
				fsd_exc_raise_sys(0);
		 }
	 }
	EXCEPT_DEFAULT
	 { fsd_free( name ); }
	FINALLY
	 {
		if( fd >= 0 )
		 {
			if( close( fd ) )
				fsd_exc_raise_sys(0);
		 }
	 }
	END_TRY

	fsd_log_return(( "=%s", name ));
	return name;
}


