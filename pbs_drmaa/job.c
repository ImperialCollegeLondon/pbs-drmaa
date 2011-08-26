/* $Id$ */
/*
 *  FedStage DRMAA for PBS Pro
 *  Copyright (C) 2006-2009  FedStage Systems
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

#ifdef HAVE_CONFIG_H
#	include <config.h>
#endif

#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <drmaa_utils/drmaa.h>
#include <drmaa_utils/drmaa_util.h>
#include <pbs_error.h>
#include <pbs_ifl.h>

#include <pbs_drmaa/job.h>
#include <pbs_drmaa/log_reader.h>
#include <pbs_drmaa/pbs_attrib.h>
#include <pbs_drmaa/session.h>
#include <pbs_drmaa/util.h>

#ifndef lint
static char rcsid[]
#	ifdef __GNUC__
		__attribute__ ((unused))
#	endif
	= "$Id$";
#endif


static void
pbsdrmaa_job_control( fsd_job_t *self, int action );

static void
pbsdrmaa_job_update_status( fsd_job_t *self );

static void
pbsdrmaa_job_on_missing( fsd_job_t *self );

void
pbsdrmaa_job_on_missing_standard( fsd_job_t *self );

void
pbsdrmaa_job_on_missing_log_based( fsd_job_t *self );

static void
pbsdrmaa_job_update( fsd_job_t *self, struct batch_status* );

bool
pbsdrmaa_job_update_status_accounting( fsd_job_t *self );


fsd_job_t *
pbsdrmaa_job_new( char *job_id )
{
	pbsdrmaa_job_t *self = (pbsdrmaa_job_t*)fsd_job_new( job_id );
	fsd_realloc( self, 1, pbsdrmaa_job_t );
	self->super.control = pbsdrmaa_job_control;
	self->super.update_status = pbsdrmaa_job_update_status;
	self->super.on_missing = pbsdrmaa_job_on_missing;
	self->update = pbsdrmaa_job_update;
	return (fsd_job_t*)self;
}


static void
pbsdrmaa_job_control( fsd_job_t *self, int action )
{
	volatile bool conn_lock = false;
	pbsdrmaa_session_t *session = (pbsdrmaa_session_t*)self->session;
	const char *job_id = self->job_id;
	const char *apicall = NULL;
	int rc = PBSE_NONE;

	fsd_log_enter(( "({job_id=%s}, action=%d)",
			self->job_id, action ));

	TRY
	 {
		int try_count;
		const int max_tries = 3;

		conn_lock = fsd_mutex_lock( &self->session->drm_connection_mutex );

		/*TODO reconnect */
		for( try_count=0;  try_count < max_tries;  try_count++ )
		 {
			switch( action )
			 {
				/*
				 * We cannot know whether we did suspend job
				 * in other way than remembering this inside DRMAA session.
				 */
				case DRMAA_CONTROL_SUSPEND:
					apicall = "pbs_sigjob";
					rc = pbs_sigjob( session->pbs_conn, (char*)job_id,
							"SIGSTOP", NULL );
					fsd_log_info(("pbs_sigjob(%s, SIGSTOP) =%d", job_id, rc));
					if( rc == PBSE_NONE )
						self->flags |= FSD_JOB_SUSPENDED;
					break;
				case DRMAA_CONTROL_RESUME:
					apicall = "pbs_sigjob";
					rc = pbs_sigjob( session->pbs_conn, (char*)job_id,
							"SIGCONT", NULL );
					fsd_log_info(("pbs_sigjob(%s, SIGCONT) =%d", job_id, rc));
					if( rc == PBSE_NONE )
						self->flags &= ~FSD_JOB_SUSPENDED;
					break;
				case DRMAA_CONTROL_HOLD:
					apicall = "pbs_holdjob";
					rc = pbs_holdjob( session->pbs_conn, (char*)job_id,
							USER_HOLD, NULL );
					fsd_log_info(("pbs_sigjob(%s, SIGHOLD) =%d", job_id, rc));
					if( rc == PBSE_NONE )
						self->flags |= FSD_JOB_HOLD;
					break;
				case DRMAA_CONTROL_RELEASE:
					apicall = "pbs_rlsjob";
					rc = pbs_rlsjob( session->pbs_conn, (char*)job_id,
							USER_HOLD, NULL );
					fsd_log_info(("pbs_rlsjob(%s) =%d", job_id, rc));
					if( rc == PBSE_NONE )
						self->flags &= FSD_JOB_HOLD;
					break;
				case DRMAA_CONTROL_TERMINATE:
					apicall = "pbs_deljob";
					rc = pbs_deljob( session->pbs_conn, (char*)job_id, NULL );
					fsd_log_info(("pbs_deljob(%s) =%d", job_id, rc));
					/* Torque:
					 * deldelay=N -- delay between SIGTERM and SIGKILL (default 0) */
					if( rc == PBSE_NONE )
					 {
						self->flags &= FSD_JOB_TERMINATED_MASK;
						if( (self->flags & FSD_JOB_TERMINATED) == 0 )
							self->flags |= FSD_JOB_TERMINATED | FSD_JOB_ABORTED;
					 }
					break;
			 }

			if( rc == PBSE_NONE )
				break;
			else if( rc == PBSE_INTERNAL )
			 {
				/*
				 * In PBS Pro pbs_sigjob raises internal server error (PBSE_INTERNAL)
				 * when job just changed its state to running.
				 */
				fsd_log_debug(( "repeating request (%d of %d)",
							try_count+2, max_tries ));
				sleep( 1 );
			 }
			else
				pbsdrmaa_exc_raise_pbs( apicall );
		 } /* end for */
	 }
	FINALLY
	 {
		if( conn_lock )
			conn_lock = fsd_mutex_unlock( &self->session->drm_connection_mutex );
	 }
	END_TRY

	fsd_log_return((""));
}


void
pbsdrmaa_job_update_status( fsd_job_t *self )
{
	volatile bool conn_lock = false;
	struct batch_status *volatile status = NULL;
	pbsdrmaa_session_t *session = (pbsdrmaa_session_t*)self->session;

	fsd_log_enter(( "({job_id=%s})", self->job_id ));
	
	TRY
	 {
		conn_lock = fsd_mutex_lock( &self->session->drm_connection_mutex );
retry:

#ifdef PBS_PROFESSIONAL
		status = pbs_statjob( session->pbs_conn, self->job_id, NULL, NULL );
#else
		status = pbs_statjob( session->pbs_conn, self->job_id, session->status_attrl, NULL );
#endif
		fsd_log_info(( "pbs_statjob(fd=%d, job_id=%s, attribs={...}) =%p",
				 session->pbs_conn, self->job_id, (void*)status ));
		if( status == NULL )
		 {
			if(pbsdrmaa_job_update_status_accounting(self) == false)
			{
	#ifndef PBS_PROFESSIONAL
				fsd_log_error(("pbs_statjob error: %d, %s, %s", pbs_errno, pbse_to_txt(pbs_errno), pbs_strerror(pbs_errno)));
	#else
	#  ifndef PBS_PROFESSIONAL_NO_LOG
				fsd_log_error(("pbs_statjob error: %d, %s", pbs_errno, pbse_to_txt(pbs_errno)));
	#  else
				fsd_log_error(("pbs_statjob error: %d", pbs_errno));
	#  endif
	#endif

				/**/

				switch( pbs_errno )
				 {
					case PBSE_UNKJOBID:
						break;
					case PBSE_PROTOCOL:
					case PBSE_EXPIRED:
						if ( session->pbs_conn >= 0 )
							pbs_disconnect( session->pbs_conn );
						sleep(1);
						session->pbs_conn = pbs_connect( session->super.contact );
						if( session->pbs_conn < 0 )
							pbsdrmaa_exc_raise_pbs( "pbs_connect" );
						else 
						 {
							fsd_log_error(("retry:"));
							goto retry;
						 }
					default:
						pbsdrmaa_exc_raise_pbs( "pbs_statjob" );
						break;
					case 0:  /* ? */
						fsd_exc_raise_code( FSD_ERRNO_INTERNAL_ERROR );
						break;
				 }
			 }
		 }

		conn_lock = fsd_mutex_unlock( &self->session->drm_connection_mutex );

		if( status != NULL )
		 {
			((pbsdrmaa_job_t*)self)->update( self, status );
		 }
		else if( self->state < DRMAA_PS_DONE )
			self->on_missing( self );
	 }
	FINALLY
	 {
		if( conn_lock )
			conn_lock = fsd_mutex_unlock( &self->session->drm_connection_mutex );
		if( status != NULL )
			pbs_statfree( status );
	 }
	END_TRY

	fsd_log_return((""));
}


void
pbsdrmaa_job_update( fsd_job_t *self, struct batch_status *status )
{
	struct attrl *attribs = status->attribs;
	struct attrl *i = NULL;
	char pbs_state = 0;
	int exit_status = -2;
	const char *cpu_usage = NULL;
	const char *mem_usage = NULL;
	const char *vmem_usage = NULL;
	const char *walltime = NULL;
	long unsigned int modify_time = 0;

	fsd_log_enter(( "({job_id=%s})", self->job_id ));
#ifdef DEBUGGING
	pbsdrmaa_dump_attrl( attribs, NULL );
#endif
	fsd_assert( !strcmp( self->job_id, status->name ) );

	for( i = attribs;  i != NULL;  i = i->next )
	 {
		int attr;
		attr = pbsdrmaa_pbs_attrib_by_name( i->name );
		switch( attr )
		 {
			case PBSDRMAA_ATTR_JOB_STATE:
				pbs_state = i->value[0];				
				break;
			case PBSDRMAA_ATTR_EXIT_STATUS:
				exit_status = atoi( i->value );
				break;
			case PBSDRMAA_ATTR_RESOURCES_USED:
				if( !strcmp( i->resource, "cput" ) )
					cpu_usage = i->value;
				else if( !strcmp( i->resource, "mem" ) )
					mem_usage = i->value;
				else if( !strcmp( i->resource, "vmem" ) )
					vmem_usage = i->value;
				else if( !strcmp( i->resource, "walltime" ) )
					walltime = i->value;
				break;
			case PBSDRMAA_ATTR_QUEUE:
				if (!self->queue)
					self->queue = fsd_strdup(i->value);
				break;
			case PBSDRMAA_ATTR_ACCOUNT_NAME:
				if (!self->project)
					self->project = fsd_strdup(i->value);
				break;
			case PBSDRMAA_ATTR_EXECUTION_HOST:
				if (!self->execution_hosts) {
					fsd_log_debug(("execution_hosts = %s", i->value));
					self->execution_hosts = fsd_strdup(i->value);
				}
				break;
			case PBSDRMAA_ATTR_START_TIME:
				{
				  long unsigned int start_time;
				  if (self->start_time == 0 && sscanf(i->value, "%lu", &start_time) == 1)
					self->start_time = start_time;
				  break;
				}
			case PBSDRMAA_ATTR_MTIME:
				if (sscanf(i->value, "%lu", &modify_time) != 1)
					modify_time = 0;
				break;
		 }
	 }

	if( pbs_state )
		fsd_log_debug(( "pbs_state: %c", pbs_state ));

	if( exit_status != -2 )
	 {
		fsd_log_debug(( "exit_status: %d", exit_status ));
		self->exit_status = exit_status;
	 }
	if(pbs_state){
		switch( pbs_state )
		 {
			case 'C': /* Job is completed after having run. */
				self->flags &= FSD_JOB_TERMINATED_MASK;
				self->flags |= FSD_JOB_TERMINATED;
				if (exit_status != -2) { /* has exit code */
					if( self->exit_status == 0) 
						self->state = DRMAA_PS_DONE;
					else 
						self->state = DRMAA_PS_FAILED;
				} else {
					self->state = DRMAA_PS_FAILED;
					self->exit_status = -1;
				}
				if (modify_time != 0)
					self->end_time = modify_time; /* take last modify time as end time */
				else
					self->end_time = time(NULL);
				
				if (self->start_time == 0)
					self->start_time = self->end_time;

				break;
			case 'E': /* Job is exiting after having run. - MM: ignore exiting state (transient state) - outputs might have not been transfered yet, 
					MM2: mark job as running if current job status is undetermined - fix "ps after job was ripped" */
				if (self->state == DRMAA_PS_UNDETERMINED)
					self->state = DRMAA_PS_RUNNING;
				break;
			case 'H': /* Job is held. */
				self->state = DRMAA_PS_USER_ON_HOLD;
				self->flags |= FSD_JOB_HOLD;
				break;
			case 'Q': /* Job is queued, eligible to run or routed. */
			case 'W': /* Job is waiting for its execution time to be reached. */
				self->state = DRMAA_PS_QUEUED_ACTIVE;
				self->flags &= ~FSD_JOB_HOLD;
				break;
			case 'R': /* Job is running. */
			case 'T': /* Job is being moved to new location (?). */
			 {
				if( self->flags & FSD_JOB_SUSPENDED )
					self->state = DRMAA_PS_USER_SUSPENDED;
				else
					self->state = DRMAA_PS_RUNNING;
				break;
			 }
			case 'S': /* (Unicos only) job is suspend. */
				self->state = DRMAA_PS_SYSTEM_SUSPENDED;
				break;
			case 0:  default:
				self->state = DRMAA_PS_UNDETERMINED;
				break;
	 }
}
	fsd_log_debug(( "job_ps: %s", drmaa_job_ps_to_str(self->state) ));

	 {
		int hours, minutes, seconds;
		long mem;
		if( cpu_usage && sscanf( cpu_usage, "%d:%d:%d", &hours, &minutes, &seconds ) == 3 )
		 {
			self->cpu_usage = 60*( 60*hours + minutes ) + seconds;
			fsd_log_debug(( "cpu_usage: %s=%lds", cpu_usage, self->cpu_usage ));
		 }
		if( mem_usage && sscanf( mem_usage, "%ldkb", &mem ) == 1 )
		 {
			self->mem_usage = 1024*mem;
			fsd_log_debug(( "mem_usage: %s=%ldB", mem_usage, self->mem_usage ));
		 }
		if( vmem_usage && sscanf( vmem_usage, "%ldkb", &mem ) == 1 )
		 {
			self->vmem_usage = 1024*mem;
			fsd_log_debug(( "vmem_usage: %s=%ldB", vmem_usage, self->vmem_usage ));
		 }
		if( walltime && sscanf( walltime, "%d:%d:%d", &hours, &minutes, &seconds ) == 3 )
		 {
			self->walltime = 60*( 60*hours + minutes ) + seconds;
			fsd_log_debug(( "walltime: %s=%lds", walltime, self->walltime ));
		 }
	 }
}

void
pbsdrmaa_job_on_missing( fsd_job_t *self )
{
	pbsdrmaa_session_t *pbssession = (pbsdrmaa_session_t*)self->session;

	if( pbssession->pbs_home == NULL || pbssession->super.wait_thread_started )
		pbsdrmaa_job_on_missing_standard( self );	
	else
		pbsdrmaa_job_on_missing_log_based( self );	
}

void
pbsdrmaa_job_on_missing_standard( fsd_job_t *self )
{
	fsd_drmaa_session_t *session = self->session;
	
	unsigned missing_mask = 0;

	fsd_log_enter(( "({job_id=%s})", self->job_id ));
	fsd_log_warning(( "Job %s missing from DRM queue", self->job_id ));

	switch( session->missing_jobs )
	{
		case FSD_REVEAL_MISSING_JOBS:         missing_mask = 0;     break;
		case FSD_IGNORE_MISSING_JOBS:         missing_mask = 0x73;  break;
		case FSD_IGNORE_QUEUED_MISSING_JOBS:  missing_mask = 0x13;  break;
	}
	fsd_log_debug(( "last job_ps: %s (0x%02x); mask: 0x%02x",
				drmaa_job_ps_to_str(self->state), self->state, missing_mask ));

	if( self->state < DRMAA_PS_DONE
			&&  (self->state & ~missing_mask) )
		fsd_exc_raise_fmt(
				FSD_DRMAA_ERRNO_INVALID_JOB,
				"self %s missing from queue", self->job_id
				);

	if( (self->flags & FSD_JOB_TERMINATED_MASK) == 0 )
	{
		self->flags &= FSD_JOB_TERMINATED_MASK;
		self->flags |= FSD_JOB_TERMINATED;
	}

	if( (self->flags & FSD_JOB_ABORTED) == 0
			&&  session->missing_jobs == FSD_IGNORE_MISSING_JOBS )
	{ /* assume everthing was ok */
		self->state = DRMAA_PS_DONE;
		self->exit_status = 0;
	}
	else
	{ /* job aborted */
		self->state = DRMAA_PS_FAILED;
		self->exit_status = -1;
	}

	fsd_cond_broadcast( &self->status_cond);

	fsd_log_return(( "; job_ps=%s, exit_status=%d",
				drmaa_job_ps_to_str(self->state), self->exit_status ));
}

void
pbsdrmaa_job_on_missing_log_based( fsd_job_t *self )
{
	fsd_drmaa_session_t *session = self->session;
	pbsdrmaa_log_reader_t *log_reader = NULL;
	
	fsd_log_enter(( "({job_id=%s})", self->job_id ));
	fsd_log_info(( "Job %s missing from DRM queue", self->job_id ));
	
	TRY
	{	
		log_reader = pbsdrmaa_log_reader_new( session, self);
		log_reader->read_log( log_reader ); 
	}
	FINALLY
	{
		pbsdrmaa_log_reader_destroy( log_reader );
	}
	END_TRY

	fsd_log_return(( "; job_ps=%s, exit_status=%d",
				drmaa_job_ps_to_str(self->state), self->exit_status ));	
}

bool
pbsdrmaa_job_update_status_accounting( fsd_job_t *self )
{
	fsd_drmaa_session_t *session = self->session;
	pbsdrmaa_log_reader_t *log_reader = NULL;
	bool res = false;
	
	fsd_log_enter(( "({job_id=%s})", self->job_id ));
	fsd_log_info(( "Reading job %s info from accounting file", self->job_id ));
	
	TRY
	{	
		log_reader = pbsdrmaa_log_reader_accounting_new( session, self);
		bool res = log_reader->read_log( log_reader ); 
	}
	FINALLY
	{
		pbsdrmaa_log_reader_destroy( log_reader );
	}
	END_TRY

	fsd_log_return((""));
	return res;
}
