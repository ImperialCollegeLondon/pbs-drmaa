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
#include <sys/stat.h>

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


static void pbsdrmaa_job_control( fsd_job_t *self, int action );

static void pbsdrmaa_job_update_status( fsd_job_t *self );

static void pbsdrmaa_job_on_missing( fsd_job_t *self );

static void pbsdrmaa_job_on_missing_standard( fsd_job_t *self );

static void pbsdrmaa_job_update( fsd_job_t *self, struct batch_status* );

static int pbsdrmaa_job_read_exit_status( const char *job_id, const char *job_state_dir_prefix);

fsd_job_t *
pbsdrmaa_job_new( char *job_id )
{
	pbsdrmaa_job_t *self = (pbsdrmaa_job_t*)fsd_job_new( job_id );
	fsd_realloc( self, 1, pbsdrmaa_job_t );
	self->super.control = pbsdrmaa_job_control;
	self->super.update_status = pbsdrmaa_job_update_status;
	self->super.on_missing = pbsdrmaa_job_on_missing;
	self->missing_time = 0;
	self->update = pbsdrmaa_job_update;
	return (fsd_job_t*)self;
}


static void
pbsdrmaa_job_control( fsd_job_t *self, int action )
{
	pbsdrmaa_session_t *session = (pbsdrmaa_session_t*)self->session;
	const char *job_id = self->job_id;

	fsd_log_enter(( "({job_id=%s}, action=%d)", self->job_id, action ));

	switch( action )
	 {
		/*
		 * We cannot know whether we did suspend job
		 * in other way than remembering this inside DRMAA session.
		 */
		case DRMAA_CONTROL_SUSPEND:
			session->pbs_connection->sigjob( session->pbs_connection, (char*)job_id, "SIGSTOP");
			self->flags |= FSD_JOB_SUSPENDED;
			break;
		case DRMAA_CONTROL_RESUME:
			session->pbs_connection->sigjob( session->pbs_connection, (char*)job_id, "SIGCONT");
			self->flags &= ~FSD_JOB_SUSPENDED;
			break;
		case DRMAA_CONTROL_HOLD:
			session->pbs_connection->holdjob( session->pbs_connection, (char*)job_id );
			self->flags |= FSD_JOB_HOLD;
			break;
		case DRMAA_CONTROL_RELEASE:
			session->pbs_connection->rlsjob( session->pbs_connection, (char*)job_id );
			self->flags &= ~FSD_JOB_HOLD;
			break;
		case DRMAA_CONTROL_TERMINATE:
			session->pbs_connection->deljob( session->pbs_connection, (char*)job_id );
			/* TODO: make deldelay configurable ???:
			 * deldelay=N -- delay between SIGTERM and SIGKILL (default 0) */
			self->flags &= FSD_JOB_TERMINATED_MASK;
			if( (self->flags & FSD_JOB_TERMINATED) == 0 )
				self->flags |= FSD_JOB_TERMINATED | FSD_JOB_ABORTED;
			break;
	 }

	fsd_log_return((""));
}


void
pbsdrmaa_job_update_status( fsd_job_t *self )
{
	struct batch_status *volatile status = NULL;
	pbsdrmaa_session_t *session = (pbsdrmaa_session_t*)self->session;

	fsd_log_enter(( "({job_id=%s})", self->job_id ));
	
	TRY
	 {

#ifdef PBS_PROFESSIONAL
		status = session->pbs_connection->statjob( session->pbs_connection, self->job_id, NULL);
#else
		status = session->pbs_connection->statjob( session->pbs_connection, self->job_id, session->status_attrl);
#endif

		if( status != NULL )
		 {
			((pbsdrmaa_job_t*)self)->update( self, status );
		 }
		else if( self->state < DRMAA_PS_DONE )
		 {
			self->on_missing( self );
		 }
	 }
	FINALLY
	 {
		if( status != NULL )
			session->pbs_connection->statjob_free( session->pbs_connection, status );
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
	int exit_status = -101;
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
				exit_status = fsd_atoi( i->value );
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
#ifndef PBS_PROFESSIONAL
			case PBSDRMAA_ATTR_EXECUTION_HOST:
				if (!self->execution_hosts) {
					fsd_log_debug(("execution_hosts = %s", i->value));
					self->execution_hosts = fsd_strdup(i->value);
				}
				break;
#else
			case PBSDRMAA_ATTR_EXECUTION_VNODE:
				if (!self->execution_hosts) {
					fsd_log_debug(("execution_hosts = %s", i->value));
					self->execution_hosts = fsd_strdup(i->value);
				}
				break;
#endif
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

	if( exit_status != -101 )
	 {
		fsd_log_debug(( "exit_status: %d", exit_status ));
		self->exit_status = exit_status;

		if (self->exit_status < 0)
		 {
			self->exit_status = -1;
			fsd_log_error(("ExitStatus = %d, probably system problem, report job %s to the local administrator", exit_status, self->job_id));
		 }

	 }
	if(pbs_state){
		switch( pbs_state )
		 {
			case 'C': /* Job is completed after having run. */
				self->flags &= FSD_JOB_TERMINATED_MASK;
				self->flags |= FSD_JOB_TERMINATED;
				if (exit_status != -101) { /* has exit code */
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
	pbsdrmaa_job_t *pbsself = (pbsdrmaa_job_t *)self;
	
	if (!pbsself->missing_time)
	 {
		pbsself->missing_time = time(NULL);
	 }

	fsd_log_info(("pbsdrmaa_job_on_missing: pbs_home=%s, wait_thread_started=%d, submit_time=%d, missing_time=%d", 
		pbssession->pbs_home, 
		pbssession->super.wait_thread_started, 
		(int)self->submit_time, 
		(int)pbsself->missing_time));
	
	#define DRMAA_MAX_MISSING_TIME (30)
	if( pbssession->pbs_home != NULL && pbssession->super.wait_thread_started && self->submit_time && (time(NULL) - pbsself->missing_time < DRMAA_MAX_MISSING_TIME))
		fsd_log_info(("Job on missing but WT is running. Skipping...")); /* TODO: try to provide implementation that uses accounting/server log files */
	else
		pbsdrmaa_job_on_missing_standard( self );	
}

void
pbsdrmaa_job_on_missing_standard( fsd_job_t *self )
{
	fsd_drmaa_session_t *session = self->session;
	pbsdrmaa_session_t *pbssession = (pbsdrmaa_session_t *)session;
	int exit_status = -1;
	
	fsd_log_enter(( "({job_id=%s})", self->job_id ));
	fsd_log_warning(( "Job %s missing from DRM queue", self->job_id ));

	fsd_log_info(( "job_on_missing: last job_ps: %s (0x%02x)", drmaa_job_ps_to_str(self->state), self->state));

	if( (exit_status = pbsdrmaa_job_read_exit_status(self->job_id, pbssession->job_exit_status_file_prefix)) == 0 )
	{
		self->state = DRMAA_PS_DONE;
		self->exit_status = exit_status;
	}
	else
	{
		self->state = DRMAA_PS_FAILED;
		self->exit_status = exit_status;
	}
        fsd_log_info(("job_on_missing evaluation result: state=%d exit_status=%d", self->state, self->exit_status));

	fsd_cond_broadcast( &self->status_cond);
	fsd_cond_broadcast( &self->session->wait_condition );

	fsd_log_return(( "; job_ps=%s, exit_status=%d", drmaa_job_ps_to_str(self->state), self->exit_status ));
}

int
pbsdrmaa_job_read_exit_status( const char *job_id, const char *job_state_dir_prefix)
{
	char *status_file = NULL, *start_file = NULL;
	FILE *fhandle = NULL;
	int exit_status = -1;

	fsd_log_enter(("({job_id=%s, job_state_dir_prefix=%s})", job_id, job_state_dir_prefix));

	status_file = fsd_asprintf("%s/%s.exitcode", job_state_dir_prefix, job_id);
	start_file = fsd_asprintf("%s/%s.started", job_state_dir_prefix, job_id);

	if ((fhandle = fopen(status_file, "r")) == NULL)
	 {
		struct stat tmpstat;

		fsd_log_error(("Failed to open job status file: %s", status_file));
		if (stat(start_file, &tmpstat) == 0 && (tmpstat.st_mode & S_IFREG))
		 {
			exit_status = 143; /* SIGTERM */
			fsd_log_info(("But start file exist %s. Assuming that job was killed (exit_status=%d).", start_file, exit_status));
		 }
		else
		 {
			fsd_log_error(("Start file not found: %s", start_file));
		 }
	

	 }
	else
	 {
		(void)fscanf(fhandle, "%d", &exit_status); /*on error exit_status == -1 */
		fclose(fhandle);
	 }

	fsd_free(status_file);
	fsd_free(start_file);

	return exit_status;
}

