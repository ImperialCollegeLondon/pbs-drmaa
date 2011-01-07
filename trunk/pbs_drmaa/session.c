/* $Id: session.c 385 2011-01-04 18:24:05Z mamonski $ */
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

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

#include <pbs_ifl.h>
#include <pbs_error.h>

#include <drmaa_utils/datetime.h>
#include <drmaa_utils/drmaa.h>
#include <drmaa_utils/iter.h>
#include <drmaa_utils/conf.h>
#include <drmaa_utils/session.h>
#include <drmaa_utils/datetime.h>

#include <pbs_drmaa/job.h>
#include <pbs_drmaa/session.h>
#include <pbs_drmaa/submit.h>
#include <pbs_drmaa/util.h>

#include <errno.h>

#ifndef lint
static char rcsid[]
#	ifdef __GNUC__
		__attribute__ ((unused))
#	endif
	= "$Id: session.c 385 2011-01-04 18:24:05Z mamonski $";
#endif

static void
pbsdrmaa_session_destroy( fsd_drmaa_session_t *self );

static void 
pbsdrmaa_session_apply_configuration( fsd_drmaa_session_t *self );

static fsd_job_t *
pbsdrmaa_session_new_job( fsd_drmaa_session_t *self, const char *job_id );

static bool
pbsdrmaa_session_do_drm_keeps_completed_jobs( pbsdrmaa_session_t *self );

static void
pbsdrmaa_session_update_all_jobs_status( fsd_drmaa_session_t *self );

static void 
*pbsdrmaa_session_wait_thread( fsd_drmaa_session_t *self );

static char *
pbsdrmaa_session_run_impl(
		fsd_drmaa_session_t *self,
		const fsd_template_t *jt,
		int bulk_idx
		);

static struct attrl *
pbsdrmaa_create_status_attrl(void);


fsd_drmaa_session_t *
pbsdrmaa_session_new( const char *contact )
{
	pbsdrmaa_session_t *volatile self = NULL;

	if( contact == NULL )
		contact = "";
	TRY
	 {
		self = (pbsdrmaa_session_t*)fsd_drmaa_session_new(contact);
		fsd_realloc( self, 1, pbsdrmaa_session_t );
		self->super_wait_thread = NULL;

		self->log_file_initial_size = 0;
		self->pbs_conn = -1;
		self->pbs_home = NULL;

		self->wait_thread_log = false;
		self->status_attrl = NULL;
		
		self->super_destroy = self->super.destroy;
		self->super.destroy = pbsdrmaa_session_destroy;
		self->super.new_job = pbsdrmaa_session_new_job;
		self->super.update_all_jobs_status
				= pbsdrmaa_session_update_all_jobs_status;
		self->super.run_impl = pbsdrmaa_session_run_impl;

		self->super_apply_configuration = self->super.apply_configuration;
		self->super.apply_configuration = pbsdrmaa_session_apply_configuration;

		self->do_drm_keeps_completed_jobs =
			pbsdrmaa_session_do_drm_keeps_completed_jobs;

		self->status_attrl = pbsdrmaa_create_status_attrl();

		self->pbs_conn = pbs_connect( self->super.contact );
		fsd_log_debug(( "pbs_connect(%s) =%d", self->super.contact,
					self->pbs_conn ));
		if( self->pbs_conn < 0 )
			pbsdrmaa_exc_raise_pbs( "pbs_connect" );

		self->super.load_configuration( &self->super, "pbs_drmaa" );

		self->super.missing_jobs = FSD_IGNORE_MISSING_JOBS;
		if( self->do_drm_keeps_completed_jobs( self ) )
			self->super.missing_jobs = FSD_IGNORE_QUEUED_MISSING_JOBS;
	 }
	EXCEPT_DEFAULT
	 {
		if( self )
		  {
			self->super.destroy( &self->super );
			self = NULL;
		  }
	 }
	END_TRY
	return (fsd_drmaa_session_t*)self;
}


void
pbsdrmaa_session_destroy( fsd_drmaa_session_t *self )
{
	pbsdrmaa_session_t *pbsself = (pbsdrmaa_session_t*)self;
	self->stop_wait_thread( self );
	if( pbsself->pbs_conn >= 0 )
		pbs_disconnect( pbsself->pbs_conn );
	fsd_free( pbsself->status_attrl );
	pbsself->super_destroy( self );	
}


static char *
pbsdrmaa_session_run_impl(
		fsd_drmaa_session_t *self,
		const fsd_template_t *jt,
		int bulk_idx
		)
{
	char *volatile job_id = NULL;
	fsd_job_t *volatile job = NULL;
	pbsdrmaa_submit_t *volatile submit = NULL;

	fsd_log_enter(( "(jt=%p, bulk_idx=%d)", (void*)jt, bulk_idx ));
	TRY
	 {
		submit = pbsdrmaa_submit_new( self, jt, bulk_idx );
		submit->eval( submit );
		job_id = submit->submit( submit );
		job = self->new_job( self, job_id );
		job->submit_time = time(NULL);
		job->flags |= FSD_JOB_CURRENT_SESSION;
		self->jobs->add( self->jobs, job );
		job->release( job );  job = NULL;
	 }
	EXCEPT_DEFAULT
	 {
		fsd_free( job_id );
		fsd_exc_reraise();
	 }
	FINALLY
	 {
		if( submit )
			submit->destroy( submit );
		if( job )
			job->release( job );
	 }
	END_TRY
	fsd_log_return(( " =%s", job_id ));
	return job_id;
}


static fsd_job_t *
pbsdrmaa_session_new_job( fsd_drmaa_session_t *self, const char *job_id )
{
	fsd_job_t *job;
	job = pbsdrmaa_job_new( fsd_strdup(job_id) );
	job->session = self;
	return job;
}

void
pbsdrmaa_session_apply_configuration( fsd_drmaa_session_t *self )
{
	pbsdrmaa_session_t *pbsself = (pbsdrmaa_session_t*)self;
	fsd_conf_option_t *pbs_home;
	pbs_home = fsd_conf_dict_get(self->configuration, "pbs_home" );
	if( pbs_home )
	 {
		if( pbs_home->type == FSD_CONF_STRING )
		 {
			struct stat statbuf;
			char * volatile log_path;
			time_t t;

			pbsself->pbs_home = pbs_home->val.string;
			fsd_log_debug(("pbs_home: %s",pbsself->pbs_home));
			pbsself->super_wait_thread = pbsself->super.wait_thread;
			pbsself->super.wait_thread = pbsdrmaa_session_wait_thread;		
			pbsself->wait_thread_log = true;
	
			time(&t);	
			localtime_r(&t,&pbsself->log_file_initial_time);

			if((log_path = fsd_asprintf("%s/server_logs/%04d%02d%02d",
    		  		pbsself->pbs_home,	 
  		    		pbsself->log_file_initial_time.tm_year + 1900,
   		    		pbsself->log_file_initial_time.tm_mon + 1,
   		   		pbsself->log_file_initial_time.tm_mday)) == NULL) {
				fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"WT - Memory allocation wasn't possible");
			}

			if(stat(log_path,&statbuf) == -1) {
				char errbuf[256] = "InternalError";
				(void)strerror_r(errno, errbuf, 256);
				fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"stat error: %s",errbuf);
			}
	
			fsd_log_debug(("Log file %s size %d",log_path,(int) statbuf.st_size));
			pbsself->log_file_initial_size = statbuf.st_size;
			fsd_free(log_path);
		 }
		else
		{
			pbsself->super.enable_wait_thread = false;
			pbsself->wait_thread_log = false;
			fsd_log_debug(("pbs_home not configured. Running standard wait_thread (pooling)."));
		}
	 }



	pbsself->super_apply_configuration(self); /* call method from the superclass */
}


void
pbsdrmaa_session_update_all_jobs_status( fsd_drmaa_session_t *self )
{
	volatile bool conn_lock = false;
	volatile bool jobs_lock = false;
	pbsdrmaa_session_t *pbsself = (pbsdrmaa_session_t*)self;
	fsd_job_set_t *jobs = self->jobs;
	struct batch_status *volatile status = NULL;

	fsd_log_enter((""));

	TRY
	 {
		conn_lock = fsd_mutex_lock( &self->drm_connection_mutex );
retry:

#ifdef PBS_PROFESSIONAL
		status = pbs_statjob( pbsself->pbs_conn, NULL, NULL, NULL );
#else
		status = pbs_statjob( pbsself->pbs_conn, NULL, pbsself->status_attrl, NULL );
#endif
		fsd_log_debug(( "pbs_statjob( fd=%d, job_id=NULL, attribs={...} ) =%p",
				 pbsself->pbs_conn, (void*)status ));
		if( status == NULL  &&  pbs_errno != 0 )
		 {
			if (pbs_errno == PBSE_PROTOCOL || pbs_errno == PBSE_EXPIRED)
			 {
				pbs_disconnect( pbsself->pbs_conn );
				sleep(1);
				pbsself->pbs_conn = pbs_connect( pbsself->super.contact );
				if( pbsself->pbs_conn < 0 )
					pbsdrmaa_exc_raise_pbs( "pbs_connect" );
				else
					goto retry;
			 }
			else
			 {
				pbsdrmaa_exc_raise_pbs( "pbs_statjob" );
			 }
		 }
		conn_lock = fsd_mutex_unlock( &self->drm_connection_mutex );

		 {
			size_t i;
			fsd_job_t *job;
			jobs_lock = fsd_mutex_lock( &jobs->mutex );
			for( i = 0;  i < jobs->tab_size;  i++ )
				for( job = jobs->tab[i];  job != NULL;  job = job->next )
				 {
					fsd_mutex_lock( &job->mutex );
					job->flags |= FSD_JOB_MISSING;
					fsd_mutex_unlock( &job->mutex );
				 }
			jobs_lock = fsd_mutex_unlock( &jobs->mutex );
		 }

		 {
			struct batch_status *volatile i;
			for( i = status;  i != NULL;  i = i->next )
			 {
				fsd_job_t *job = NULL;
				fsd_log_debug(( "job_id=%s", i->name ));
				job = self->get_job( self, i->name );
				if( job != NULL )
				 {
					job->flags &= ~FSD_JOB_MISSING;
					TRY
					 {
						((pbsdrmaa_job_t*)job)->update( job, i );
					 }
					FINALLY
					 {
						job->release( job );
					 }
					END_TRY
				 }
			 }
		 }

		 {
			size_t volatile i;
			fsd_job_t *volatile job;
			jobs_lock = fsd_mutex_lock( &jobs->mutex );
			for( i = 0;  i < jobs->tab_size;  i++ )
				for( job = jobs->tab[i];  job != NULL;  job = job->next )
				 {
					fsd_mutex_lock( &job->mutex );
					TRY
					 {
						if( job->flags & FSD_JOB_MISSING )
							job->on_missing( job );
					 }
					FINALLY{ fsd_mutex_unlock( &job->mutex ); }
					END_TRY
				 }
			jobs_lock = fsd_mutex_unlock( &jobs->mutex );
		 }
	 }
	FINALLY
	 {
		if( status != NULL )
			pbs_statfree( status );
		if( conn_lock )
			conn_lock = fsd_mutex_unlock( &self->drm_connection_mutex );
		if( jobs_lock )
			jobs_lock = fsd_mutex_unlock( &jobs->mutex );
	 }
	END_TRY

	fsd_log_return((""));
}



struct attrl *
pbsdrmaa_create_status_attrl(void)
{
	struct attrl *result = NULL;
	struct attrl *i;
	const int max_attribs = 16;
	int n_attribs;
	int j = 0;

	fsd_log_enter((""));
	fsd_calloc( result, max_attribs, struct attrl );
	result[j++].name="job_state";
	result[j++].name="exit_status";
	result[j++].name="resources_used";
	result[j++].name="ctime";
	result[j++].name="mtime";
	result[j++].name="qtime";
	result[j++].name="etime";

	result[j++].name="queue";
	result[j++].name="Account_Name";
	result[j++].name="exec_host";
	result[j++].name="start_time";
	result[j++].name="mtime";
#if 0
	result[j].name="resources_used";  result[j].resource="walltime";  j++;
	result[j].name="resources_used";  result[j].resource="cput";  j++;
	result[j].name="resources_used";  result[j].resource="mem";  j++;
	result[j].name="resources_used";  result[j].resource="vmem";  j++;
	result[j].name="Resource_List";  result[j].resource="walltime";  j++;
	result[j].name="Resource_List";  result[j].resource="cput";  j++;
	result[j].name="Resource_List";  result[j].resource="mem";  j++;
	result[j].name="Resource_List";  result[j].resource="vmem";  j++;
#endif
	n_attribs = j;
	for( i = result;  true;  i++ )
		if( i+1 < result + n_attribs )
			i->next = i+1;
		else
		 {
			i->next = NULL;
			break;
		 }

#ifdef DEBUGGING
	fsd_log_return((":"));
	pbsdrmaa_dump_attrl( result, NULL );
#endif
	return result;
}


bool
pbsdrmaa_session_do_drm_keeps_completed_jobs( pbsdrmaa_session_t *self )
{

#ifndef PBS_PROFESSIONAL
	struct attrl default_queue_query;
	struct attrl keep_completed_query;
	struct batch_status *default_queue_result = NULL;
	struct batch_status *keep_completed_result = NULL;
	const char *default_queue = NULL;
	const char *keep_completed = NULL;
	volatile bool result = false;
	volatile bool conn_lock = false;

	TRY
	 {
		default_queue_query.next = NULL;
		default_queue_query.name = "default_queue";
		default_queue_query.resource = NULL;
		default_queue_query.value = NULL;
		keep_completed_query.next = NULL;
		keep_completed_query.name = "keep_completed";
		keep_completed_query.resource = NULL;
		keep_completed_query.value = NULL;

		conn_lock = fsd_mutex_lock( &self->super.drm_connection_mutex );

		default_queue_result =
				pbs_statserver( self->pbs_conn, &default_queue_query, NULL );
		if( default_queue_result == NULL )
			pbsdrmaa_exc_raise_pbs( "pbs_statserver" );
		if( default_queue_result->attribs
				&&  !strcmp( default_queue_result->attribs->name,
					"default_queue" ) )
			default_queue = default_queue_result->attribs->value;

		fsd_log_debug(( "default_queue: %s", default_queue ));

		if( default_queue )
		 {
			keep_completed_result = pbs_statque( self->pbs_conn,
					(char*)default_queue, &keep_completed_query, NULL );
			if( keep_completed_result == NULL )
				pbsdrmaa_exc_raise_pbs( "pbs_statque" );
			if( keep_completed_result->attribs
					&&  !strcmp( keep_completed_result->attribs->name,
						"keep_completed" ) )
				keep_completed = keep_completed_result->attribs->value;
		 }

		fsd_log_debug(( "keep_completed: %s", keep_completed ));
	 }
	EXCEPT_DEFAULT
	 {
		const fsd_exc_t *e = fsd_exc_get();
		fsd_log_warning(( "PBS server seems not to keep completed jobs\n"
				"detail: %s", e->message(e) ));
		result = false;
	 }
	ELSE
	 {
		result = false;
		if( default_queue == NULL )
			fsd_log_warning(( "no default queue set on PBS server" ));
		else if( keep_completed == NULL && self->pbs_home == NULL )
			fsd_log_warning(( "PBS server is not configured to keep completed jobs\n"
						"in Torque: set keep_completed parameter of default queue\n"
						"  $ qmgr -c 'set queue batch keep_completed = 60'\n"
						" or configure DRMAA to utilize log files"
						));
		else
			result = true;
	 }
	FINALLY
	 {
		if( default_queue_result )
			pbs_statfree( default_queue_result );
		if( keep_completed_result )
			pbs_statfree( keep_completed_result );
		if( conn_lock )
			conn_lock = fsd_mutex_unlock( &self->super.drm_connection_mutex );

	 }
	END_TRY
#endif
	return false;
}


enum field
  {
  FLD_DATE = 0,
  FLD_EVENT = 1,
  FLD_OBJ = 2,
  FLD_TYPE = 3,
  FLD_ID = 4,
  FLD_MSG = 5
  };

enum field_msg
  {
  FLD_MSG_EXIT_STATUS = 0,
  FLD_MSG_CPUT = 1,
  FLD_MSG_MEM = 2,
  FLD_MSG_VMEM = 3,
  FLD_MSG_WALLTIME = 4
  };

#define FLD_MSG_STATUS "0010"
#define FLD_MSG_STATE "0008"
#define FLD_MSG_LOG "0002"

ssize_t fsd_getline(char * line,ssize_t size, int fd)
{
	char buf;
	char * ptr = NULL;
	ssize_t n = 0, rc;
	ptr = line;
	for(n = 1; n< size; n++)
	{		
		if( (rc = read(fd,&buf,1 )) == 1) {
			*ptr++ = buf;
			if(buf == '\n')
			{
				break;
			}
		}
		else if (rc == 0) {
			if (n == 1)
				return 0;
			else
				break;
		}		
		else
			return -1; 
	}

	return n;
} 

void *
pbsdrmaa_session_wait_thread( fsd_drmaa_session_t *self )
{
	pbsdrmaa_session_t *pbsself = (pbsdrmaa_session_t*) self;
	fsd_job_t *volatile job = NULL;
	pbsdrmaa_job_t *volatile pbsjob = NULL;
	char job_id[256] = "";
	char event[256] = "";
	time_t t;
	struct tm tm;

	tm = pbsself->log_file_initial_time;

	fsd_log_enter(( "" ));
	fsd_mutex_lock( &self->mutex );
	TRY
	 {	
		char * volatile log_path = NULL;
		char buffer[4096] = "";
		bool volatile date_changed = true;
		int  volatile fd = -1;
		bool first_open = true;

		fsd_log_debug(("WT - reading log files"));

		while( self->wait_thread_run_flag )
		TRY
		 {			
			if(date_changed)
			{
				int num_tries = 0;
				
				time(&t);	
				localtime_r(&t,&tm);
			
				#define DRMAA_WAIT_THREAD_MAX_TRIES (12)
				/* generate new date, close file and open new */
				if((log_path = fsd_asprintf("%s/server_logs/%04d%02d%02d",
       					pbsself->pbs_home,	 
       					tm.tm_year + 1900,
       					tm.tm_mon + 1,
      					tm.tm_mday)) == NULL) {
					fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"WT - Memory allocation wasn't possible");
				}

				if(fd != -1)
					close(fd);

				fsd_log_debug(("Log file: %s",log_path));
				
		retry:
				if((fd = open(log_path,O_RDONLY) ) == -1 && num_tries > DRMAA_WAIT_THREAD_MAX_TRIES )
				{
					fsd_log_error(("Can't open log file. Verify pbs_home. Running standard wait_thread."));
					fsd_log_error(("Remember that without keep_completed set standard wait_thread won't run correctly"));
					/*pbsself->super.enable_wait_thread = false;*/ /* run not wait_thread */
					pbsself->wait_thread_log = false;
					pbsself->super.wait_thread = pbsself->super_wait_thread;
					pbsself->super.wait_thread(self);
				} else if ( fd == -1 ) {
					fsd_log_warning(("Can't open log file: %s. Retries count: %d", log_path, num_tries));
					num_tries++;
					sleep(5);
					goto retry;
				}

				fsd_free(log_path);

				fsd_log_debug(("Log file opened"));

				if(first_open) {
					fsd_log_debug(("Log file lseek"));
					if(lseek(fd,pbsself->log_file_initial_size,SEEK_SET) == (off_t) -1) {
						char errbuf[256] = "InternalError";
						(void)strerror_r(errno, errbuf, 256);
						fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"lseek error: %s",errbuf);
					}
					first_open = false;
				}

				date_changed = false;
			}				
			
			while ((fsd_getline(buffer,sizeof(buffer),fd)) > 0) 			
			{
				const char *volatile ptr = buffer;
  				char field[256] = "";
				int volatile field_n = 0;
 				int n;

				bool volatile job_id_match = false;
				bool volatile event_match = false;
				bool volatile log_event = false;
				bool volatile log_match = false;
  				char *  temp_date = NULL;
				

				struct batch_status status;
				status.next = NULL;

				if( strlcpy(job_id,"",sizeof(job_id)) > sizeof(job_id) ) {
					fsd_log_error(("WT - strlcpy error"));
				}
				if( strlcpy(event,"",sizeof(event)) > sizeof(event) ) {
					fsd_log_error(("WT - strlcpy error"));
				}
				while ( sscanf(ptr, "%255[^;]%n", field, &n) == 1 ) /* divide current line into fields */
				{
					if(field_n == FLD_DATE)
					{
						temp_date = fsd_strdup(field);
					}
					else if(field_n == FLD_EVENT && (strcmp(field,FLD_MSG_STATUS) == 0 || 
						     		    strcmp(field,FLD_MSG_STATE) == 0 ))
					{
						/* event described by log line*/
						if(strlcpy(event, field,sizeof(event)) > sizeof(event)) {
							fsd_log_error(("WT - strlcpy error"));
						}
						event_match = true;									
					}
					else if(event_match && field_n == FLD_ID)
					{	
						TRY
						{	
							job = self->get_job( self, field );
							pbsjob = (pbsdrmaa_job_t*) job;

							if( job )
							{
								if(strlcpy(job_id, field,sizeof(job_id)) > sizeof(job_id)) {
									fsd_log_error(("WT - strlcpy error"));
								}
								fsd_log_debug(("WT - job_id: %s",job_id));
								status.name = fsd_strdup(job_id);
								job_id_match = true; /* job_id is in drmaa */
							}
							else 
							{
								fsd_log_debug(("WT - Unknown job: %s", field));
							}
						}
						END_TRY	
					}
					else if(job_id_match && field_n == FLD_MSG)
					{						
						/* parse msg - depends on FLD_EVENT*/
						struct attrl struct_resource_cput,struct_resource_mem,struct_resource_vmem,
							struct_resource_walltime, struct_status, struct_state, struct_start_time,struct_mtime, struct_queue, struct_account_name;	
						
						bool state_running = false;

						struct_status.name = NULL;
						struct_status.value = NULL;
						struct_status.next = NULL;
						struct_status.resource = NULL;

						struct_state.name = NULL;
						struct_state.value = NULL;
						struct_state.next = NULL;
						struct_state.resource = NULL;

						struct_resource_cput.name = NULL;
						struct_resource_cput.value = NULL;
						struct_resource_cput.next = NULL;
						struct_resource_cput.resource = NULL;

						struct_resource_mem.name = NULL;
						struct_resource_mem.value = NULL;
						struct_resource_mem.next = NULL;
						struct_resource_mem.resource = NULL;

						struct_resource_vmem.name = NULL;
						struct_resource_vmem.value = NULL;
						struct_resource_vmem.next = NULL;
						struct_resource_vmem.resource = NULL;

						struct_resource_walltime.name = NULL;
						struct_resource_walltime.value = NULL;
						struct_resource_walltime.next = NULL;
						struct_resource_walltime.resource = NULL;

						struct_start_time.name = NULL;
						struct_start_time.value = NULL;
						struct_start_time.next = NULL;
						struct_start_time.resource = NULL;

						struct_mtime.name = NULL;
						struct_mtime.value = NULL;
						struct_mtime.next = NULL;
						struct_mtime.resource = NULL;

						struct_queue.name = NULL;
						struct_queue.value = NULL;
						struct_queue.next = NULL;
						struct_queue.resource = NULL;

						struct_account_name.name = NULL;
						struct_account_name.value = NULL;
						struct_account_name.next = NULL;
						struct_account_name.resource = NULL;

								
						if (strcmp(event,FLD_MSG_STATE) == 0) 
						{
							/* job run, modified, queued etc */
							int n = 0;
							status.attribs = &struct_state;
							struct_state.next = NULL;
							struct_state.name = "job_state";
							if(field[0] == 'J') /* Job Queued, Job Modified, Job Run*/
							{
								n = 4;								
							}		
							if(field[4] == 'M') {
								struct tm temp_time_tm;
								memset(&temp_time_tm, 0, sizeof(temp_time_tm));
								temp_time_tm.tm_isdst = -1;

								if (strptime(temp_date, "%m/%d/%Y %H:%M:%S", &temp_time_tm) == NULL) 
								 {
								 	fsd_log_error(("failed to parse mtime: %s", temp_date));
								 }
								else
								 {
									time_t temp_time = mktime(&temp_time_tm);
									status.attribs = &struct_mtime; 
									struct_mtime.name = "mtime";
									struct_mtime.next = NULL;
									struct_mtime.value = fsd_asprintf("%lu",temp_time);
								 }
							}		
							/* != Job deleted and Job to be deleted*/
							#ifdef PBS_PROFESSIONAL
							else if	(field[4] != 't' && field[10] != 'd') {
							#else	 	
							else if(field[4] != 'd') {
							#endif 

								if ((struct_state.value = fsd_asprintf("%c",field[n]) ) == NULL ) { /* 4 first letter of state */
									fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"WT - Memory allocation wasn't possible");
								}
								if(struct_state.value[0] == 'R'){
									state_running = true;
								}
							}
							else { /* job terminated - pbs drmaa detects failed as completed with exit_status !=0, aborted with status -1*/
								struct_status.name = "exit_status";
								struct_status.value = fsd_strdup("-1");
								struct_status.next = NULL;
								struct_state.next = &struct_status;
								struct_state.value = fsd_strdup("C");								
							}
						} 						     
						else /*if (strcmp(event,FLD_MSG_STATUS) == 0 )*/
						{
							/* exit status and rusage */
							const char *ptr2 = field;
							char  msg[ 256 ] = "";
							int n2;
							int msg_field_n = 0;
							
							struct_resource_cput.name = "resources_used";
							struct_resource_mem.name = "resources_used";
							struct_resource_vmem.name = "resources_used";
							struct_resource_walltime.name = "resources_used";
							struct_status.name = "exit_status";
							struct_state.name = "job_state";
				
							status.attribs = &struct_resource_cput;
							struct_resource_cput.next = &struct_resource_mem;
							struct_resource_mem.next = &struct_resource_vmem;
							struct_resource_vmem.next = &struct_resource_walltime;
							struct_resource_walltime.next =  &struct_status;
							struct_status.next = &struct_state;
							struct_state.next = NULL;

							while ( sscanf(ptr2, "%255[^ ]%n", msg, &n2) == 1 )
							 {						
								switch(msg_field_n) 
								{
									case FLD_MSG_EXIT_STATUS:
										struct_status.value = fsd_strdup(strchr(msg,'=')+1);
										break;

									case FLD_MSG_CPUT:
										struct_resource_cput.resource = "cput";
										struct_resource_cput.value = fsd_strdup(strchr(msg,'=')+1);
										break;

									case FLD_MSG_MEM:
										struct_resource_mem.resource = "mem";
										struct_resource_mem.value  = fsd_strdup(strchr(msg,'=')+1);
										break;

									case FLD_MSG_VMEM:
										struct_resource_vmem.resource = "vmem";
										struct_resource_vmem.value  = fsd_strdup(strchr(msg,'=')+1);
										break; 

									case FLD_MSG_WALLTIME:
										struct_resource_walltime.resource = "walltime";
										struct_resource_walltime.value  = fsd_strdup(strchr(msg,'=')+1);
										break; 
								}
							      
								ptr2 += n2; 
								msg_field_n++;
								if ( *ptr2 != ' ' )
							      	 {
									 break; 
							  	 }
							 	++ptr2;						
							 }
							struct_state.value = fsd_strdup("C");	/* we got exit_status so we say that it has completed */
						}						

						if ( state_running )
						 {
							fsd_log_debug(("WT - forcing update of job: %s", job->job_id ));
							job->update_status( job );
						 }
						else
						 {
							fsd_log_debug(("WT - updating job: %s", job->job_id ));
							pbsjob->update( job, &status );
						 }

				
						fsd_cond_broadcast( &job->status_cond);
						fsd_cond_broadcast( &self->wait_condition );

						if ( job )
							job->release( job );
	
						fsd_free(struct_resource_cput.value);
						fsd_free(struct_resource_mem.value);
						fsd_free(struct_resource_vmem.value);
						fsd_free(struct_resource_walltime.value);
						fsd_free(struct_status.value);
						fsd_free(struct_state.value);
						fsd_free(struct_start_time.value);
						fsd_free(struct_mtime.value);
						fsd_free(struct_queue.value);
						fsd_free(struct_account_name.value);

						if ( status.name!=NULL ) 
							fsd_free(status.name);
					}
					else if(field_n == FLD_EVENT && strcmp(field,FLD_MSG_LOG) == 0)
					{
						log_event = true;					
					}
					else if (log_event && field_n == FLD_ID && strcmp(field,"Log") == 0 )
					{
						log_match = true;
						log_event = false;
					}
					else if( log_match && field_n == FLD_MSG && 
						field[0] == 'L' && 
						field[1] == 'o' && 
						field[2] == 'g' && 
						field[3] == ' ' && 
						field[4] == 'c' && 
						field[5] == 'l' && 
						field[6] == 'o' && 
						field[7] == 's' && 
						field[8] == 'e' && 
						field[9] == 'd' )  /* last field in the file - strange bahaviour*/
					{
						fsd_log_debug(("WT - Date changed. Closing log file"));
						date_changed = true;
						log_match = false;
					}
					
					ptr += n; 
					if ( *ptr != ';' )
					{
						break; /* end of line */
					}
					field_n++;
					++ptr;
				}		

				if( strlcpy(buffer,"",sizeof(buffer)) > sizeof(buffer) ) {
					fsd_log_error(("WT - strlcpy error"));
				}

				fsd_free(temp_date);			
			}

			fsd_mutex_unlock( &self->mutex );	
			usleep(1000000);
			fsd_mutex_lock( &self->mutex );
		 }
		EXCEPT_DEFAULT
		 {
			const fsd_exc_t *e = fsd_exc_get();

			fsd_log_error(( "wait thread: <%d:%s>", e->code(e), e->message(e) ));
			fsd_exc_reraise();
		 }
		END_TRY

		if(fd != -1)
			close(fd);
		fsd_log_debug(("Log file closed"));
	 }
	FINALLY
	 {
	 	fsd_log_debug(("WT - Terminated."));	
	 	fsd_mutex_unlock( &self->mutex );
	 }
	END_TRY

	fsd_log_return(( " =NULL" ));
	return NULL;
}
