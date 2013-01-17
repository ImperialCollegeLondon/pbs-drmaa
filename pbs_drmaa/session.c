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

#include <pwd.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <signal.h>

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
	= "$Id$";
#endif

static void
pbsdrmaa_session_destroy( fsd_drmaa_session_t *self );

static void 
pbsdrmaa_session_apply_configuration( fsd_drmaa_session_t *self );

static fsd_job_t *
pbsdrmaa_session_new_job( fsd_drmaa_session_t *self, const char *job_id );

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

		self->status_attrl = pbsdrmaa_create_status_attrl();
		self->max_retries_count = 3;
		self->wait_thread_sleep_time = 1;
		self->job_exit_status_file_prefix = NULL;

		self->super.load_configuration( &self->super, "pbs_drmaa" );

		self->super.missing_jobs = FSD_IGNORE_MISSING_JOBS;

		self->pbs_connection = pbsdrmaa_pbs_conn_new( (fsd_drmaa_session_t *)self, contact );
		self->connection_max_lifetime =  30; /* 30 seconds */

	 }
	EXCEPT_DEFAULT
	 {
		if( self )
		  {
			self->super.destroy( &self->super );
			self = NULL;
		  }

		fsd_exc_reraise();
	 }
	END_TRY
	return (fsd_drmaa_session_t*)self;
}


void
pbsdrmaa_session_destroy( fsd_drmaa_session_t *self )
{
	pbsdrmaa_session_t *pbsself = (pbsdrmaa_session_t*)self;
	self->stop_wait_thread( self );
	pbsdrmaa_pbs_conn_destroy(pbsself->pbs_connection);
	fsd_free( pbsself->status_attrl );
	fsd_free( pbsself->job_exit_status_file_prefix );

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
	fsd_conf_option_t *pbs_home = NULL;
	fsd_conf_option_t *wait_thread_sleep_time = NULL;
	fsd_conf_option_t *max_retries_count = NULL;
	fsd_conf_option_t *user_state_dir = NULL;
	fsd_conf_option_t *connection_max_lifetime = NULL;


	pbs_home = fsd_conf_dict_get(self->configuration, "pbs_home" );
	wait_thread_sleep_time = fsd_conf_dict_get(self->configuration, "wait_thread_sleep_time" );
	max_retries_count = fsd_conf_dict_get(self->configuration, "max_retries_count" );
	user_state_dir = fsd_conf_dict_get(self->configuration, "user_state_dir" );
	connection_max_lifetime = fsd_conf_dict_get(self->configuration, "connection_max_lifetime");

	if( pbs_home && pbs_home->type == FSD_CONF_STRING )
	  {
			struct stat statbuf;
			char * volatile log_path;
			struct tm tm;
			
			pbsself->pbs_home = pbs_home->val.string;
			fsd_log_info(("pbs_home: %s",pbsself->pbs_home));
			pbsself->super_wait_thread = pbsself->super.wait_thread;
			pbsself->super.wait_thread = pbsdrmaa_session_wait_thread;		
			pbsself->wait_thread_log = true;
	
			time(&pbsself->log_file_initial_time);	
			localtime_r(&pbsself->log_file_initial_time,&tm);

			log_path = fsd_asprintf("%s/server_logs/%04d%02d%02d",
					pbsself->pbs_home,
					tm.tm_year + 1900,
					tm.tm_mon + 1,
					tm.tm_mday);

			if(stat(log_path,&statbuf) == -1)
			  {
				char errbuf[256] = "InternalError";
				(void)strerror_r(errno, errbuf, sizeof(errbuf));
				fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"stat error on file %s: %s", log_path, errbuf);
			  }
	
			fsd_log_debug(("Log file %s size %d",log_path,(int) statbuf.st_size));
			pbsself->log_file_initial_size = statbuf.st_size;
			fsd_free(log_path);
	  }

	if ( max_retries_count && max_retries_count->type == FSD_CONF_INTEGER)
	  {
		pbsself->max_retries_count = max_retries_count->val.integer;
		fsd_log_info(("Max retries count: %d", pbsself->max_retries_count));
	  }

	if ( connection_max_lifetime && connection_max_lifetime->type == FSD_CONF_INTEGER)
	  {
		pbsself->connection_max_lifetime = connection_max_lifetime->val.integer;
		fsd_log_info(("Max connection lifetime: %d", pbsself->connection_max_lifetime));
	  }

	if ( wait_thread_sleep_time && wait_thread_sleep_time->type == FSD_CONF_INTEGER)
	  {
		pbsself->wait_thread_sleep_time = wait_thread_sleep_time->val.integer;
		fsd_log_info(("Wait thread sleep time: %d", pbsself->wait_thread_sleep_time));
	  }

	if( user_state_dir && user_state_dir->type == FSD_CONF_STRING )
	  {
		struct passwd *pw = NULL;
		uid_t uid;

		uid = geteuid();
		pw = getpwuid(uid); /* drmaa_init is always called in thread safely fashion */

		if (!pw)
			fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"Failed to get pw_name of the user %d", uid);

		pbsself->job_exit_status_file_prefix = fsd_asprintf(user_state_dir->val.string, pw->pw_name);
	  }
	else
	  {
		pbsself->job_exit_status_file_prefix = fsd_asprintf("%s/.drmaa", getenv("HOME"));
	  }

	fsd_log_debug(("Trying to create state directory: %s", pbsself->job_exit_status_file_prefix));

	if (mkdir(pbsself->job_exit_status_file_prefix, 0700) == -1 && errno != EEXIST) /* TODO it would be much better to do stat before */
	  {
		fsd_log_warning(("Failed to create job state directory: %s. Valid job exit status may not be available in some cases.", pbsself->job_exit_status_file_prefix));
	  }


	/* TODO purge old exit statuses files */

	pbsself->super_apply_configuration(self); /* call method from the superclass */
}


void
pbsdrmaa_session_update_all_jobs_status( fsd_drmaa_session_t *self )
{
	volatile bool jobs_lock = false;
	pbsdrmaa_session_t *pbsself = (pbsdrmaa_session_t*)self;
	fsd_job_set_t *jobs = self->jobs;
	struct batch_status *volatile status = NULL;

	fsd_log_enter((""));

	TRY
	 {

/* TODO: query only for user's jobs pbs_selstat + ATTR_u */
#ifdef PBS_PROFESSIONAL
		status = pbsself->pbs_connection->statjob(pbsself->pbs_connection, NULL, NULL);
#else
		status = pbsself->pbs_connection->statjob(pbsself->pbs_connection, NULL, pbsself->status_attrl);
#endif

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
			 pbsself->pbs_connection->statjob_free(pbsself->pbs_connection, status );
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

void *
pbsdrmaa_session_wait_thread( fsd_drmaa_session_t *self )
{
	pbsdrmaa_log_reader_t *log_reader = NULL;
	
	fsd_log_enter(( "" ));
	
	TRY
	{	
		log_reader = pbsdrmaa_log_reader_new( self );
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
