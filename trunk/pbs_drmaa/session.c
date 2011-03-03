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
#include <pbs_drmaa/log_reader.h>
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
			struct tm tm;
			
			pbsself->pbs_home = pbs_home->val.string;
			fsd_log_debug(("pbs_home: %s",pbsself->pbs_home));
			pbsself->super_wait_thread = pbsself->super.wait_thread;
			pbsself->super.wait_thread = pbsdrmaa_session_wait_thread;		
			pbsself->wait_thread_log = true;
	
			time(&pbsself->log_file_initial_time);	
			localtime_r(&pbsself->log_file_initial_time,&tm);

			if((log_path = fsd_asprintf("%s/server_logs/%04d%02d%02d",
    		  		pbsself->pbs_home,	 
  		    		tm.tm_year + 1900,
   		    		tm.tm_mon + 1,
					tm.tm_mday)) == NULL) {
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
		fsd_log_info(( "pbs_statjob( fd=%d, job_id=NULL, attribs={...} ) =%p",
				 pbsself->pbs_conn, (void*)status ));
		if( status == NULL  &&  pbs_errno != 0 )
		 {
			if (pbs_errno == PBSE_PROTOCOL || pbs_errno == PBSE_EXPIRED)
			 {
				if ( pbsself->pbs_conn >= 0)
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

void *
pbsdrmaa_session_wait_thread( fsd_drmaa_session_t *self )
{
	pbsdrmaa_log_reader_t *log_reader = NULL;
	
	fsd_log_enter(( "" ));
	
	TRY
	{	
		log_reader = pbsdrmaa_log_reader_new( self, NULL);
		log_reader->read_log( log_reader );
	}
	FINALLY
	{
		pbsdrmaa_log_reader_destroy( log_reader );
	}
	END_TRY
	
	fsd_log_return(( " =NULL" ));
	return NULL;
}
