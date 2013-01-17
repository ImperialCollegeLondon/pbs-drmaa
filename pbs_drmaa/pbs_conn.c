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

 #ifdef HAVE_CONFIG_H
#	include <config.h>
#endif

#include <pbs_error.h>

#include <drmaa_utils/datetime.h>
#include <drmaa_utils/drmaa.h>
#include <drmaa_utils/iter.h>
#include <drmaa_utils/conf.h>
#include <drmaa_utils/datetime.h>

#include <pbs_drmaa/session.h>
#include <pbs_drmaa/pbs_conn.h>
#include <pbs_drmaa/util.h>

#include <errno.h>
#include <signal.h>
#include <unistd.h>


static char* pbsdrmaa_pbs_submit( pbsdrmaa_pbs_conn_t *self, struct attropl *attrib, char *script, char *destination );

static struct batch_status* pbsdrmaa_pbs_statjob( pbsdrmaa_pbs_conn_t *self,  char *job_id, struct attrl *attrib );

static void pbsdrmaa_pbs_statjob_free( pbsdrmaa_pbs_conn_t *self, struct batch_status* job_status );

static void pbsdrmaa_pbs_sigjob( pbsdrmaa_pbs_conn_t *self, char *job_id, char *signal );

static void pbsdrmaa_pbs_deljob( pbsdrmaa_pbs_conn_t *self,  char *job_id );

static void pbsdrmaa_pbs_rlsjob( pbsdrmaa_pbs_conn_t *self, char *job_id );

static void pbsdrmaa_pbs_holdjob( pbsdrmaa_pbs_conn_t *self,  char *job_id );

static void pbsdrmaa_pbs_connection_autoclose_thread_loop( pbsdrmaa_pbs_conn_t *self, bool reconnect);


static void check_reconnect( pbsdrmaa_pbs_conn_t *self, bool reconnect);

static void start_autoclose_thread( pbsdrmaa_pbs_conn_t *self );

static void stop_autoclose_thread( pbsdrmaa_pbs_conn_t *self );


#if defined PBS_PROFESSIONAL && defined PBSE_HISTJOBID
	#define IS_MISSING_JOB (pbs_errno == PBSE_UNKJOBID || pbs_errno == PBSE_HISTJOBID)
#else
	#define IS_MISSING_JOB (pbs_errno == PBSE_UNKJOBID)
#endif
#define IS_TRANSIENT_ERROR (pbs_errno == PBSE_PROTOCOL || pbs_errno == PBSE_EXPIRED || pbs_errno == PBSOLDE_PROTOCOL || pbs_errno == PBSOLDE_EXPIRED)

pbsdrmaa_pbs_conn_t * 
pbsdrmaa_pbs_conn_new( fsd_drmaa_session_t *session, const char *server )
{
	pbsdrmaa_pbs_conn_t *volatile self = NULL;

	fsd_log_enter((""));

	TRY
	  {
		fsd_malloc(self, pbsdrmaa_pbs_conn_t );
		
		self->session = session;
		
		self->submit = pbsdrmaa_pbs_submit;
		self->statjob = pbsdrmaa_pbs_statjob;
		self->statjob_free = pbsdrmaa_pbs_statjob_free;
		self->sigjob = pbsdrmaa_pbs_sigjob;
		self->deljob = pbsdrmaa_pbs_deljob;
		self->rlsjob = pbsdrmaa_pbs_rlsjob;
		self->holdjob = pbsdrmaa_pbs_holdjob;

		self->server = fsd_strdup(server);

		self->connection_fd = -1;

		/*ignore SIGPIPE - otherwise pbs_disconnect cause the program to exit */
		signal(SIGPIPE, SIG_IGN);	

		check_reconnect(self, false);
	  }
	EXCEPT_DEFAULT
	  {
		if( self != NULL)
		  {
			fsd_free(self->server);
			fsd_free(self);

			if (self->connection_fd != -1)
				pbs_disconnect(self->connection_fd);
			stop_autoclose_thread(self);
		  }
			
		fsd_exc_reraise();
	  }
	END_TRY

	fsd_log_return((""));

	return self;
}

void
pbsdrmaa_pbs_conn_destroy ( pbsdrmaa_pbs_conn_t * self )
{
	fsd_log_enter((""));

	TRY
	{
		if(self != NULL)
		{
			fsd_free(self->server);
			fsd_free(self);	

			if (self->connection_fd != -1)
				pbs_disconnect(self->connection_fd);
		}
	}
	EXCEPT_DEFAULT
	{
		fsd_exc_reraise();
	}
	END_TRY
	
	fsd_log_return((""));
}

char* 
pbsdrmaa_pbs_submit( pbsdrmaa_pbs_conn_t *self, struct attropl *attrib, char *script, char *destination )
{
	char *volatile job_id = NULL;
	volatile bool first_try = true;
	volatile bool conn_lock = false;

	fsd_log_enter((""));

	TRY
	 {
		conn_lock = fsd_mutex_lock(&self->session->drm_connection_mutex);

		check_reconnect(self, false);

retry:
		job_id = pbs_submit(self->connection_fd, attrib, script, destination, NULL);

		fsd_log_info(("pbs_submit(%s, %s) = %s", script, destination, job_id));

		if(job_id == NULL)
		 {
			fsd_log_error(( "pbs_submit failed, pbs_errno = %d", pbs_errno ));
			if (IS_TRANSIENT_ERROR && first_try)
			 {
				check_reconnect(self, true);
				first_try = false;
				goto retry;
			 }
			else
			 {
				pbsdrmaa_exc_raise_pbs( "pbs_submit", self->connection_fd);
			 }
		 }
	 }
	EXCEPT_DEFAULT
	 {
		fsd_free(job_id);
		fsd_exc_reraise();
	 }
	FINALLY
	 {
		if(conn_lock)
			conn_lock = fsd_mutex_unlock(&self->session->drm_connection_mutex);
	 }
	END_TRY


	fsd_log_return(("%s", job_id));

	return job_id;
}

struct batch_status* 
pbsdrmaa_pbs_statjob( pbsdrmaa_pbs_conn_t *self,  char *job_id, struct attrl *attrib )
{
	struct batch_status *volatile status = NULL;
	volatile bool first_try = true;
	volatile bool conn_lock = false;


	fsd_log_enter((""));

	TRY
	 {
		conn_lock = fsd_mutex_lock(&self->session->drm_connection_mutex);

		check_reconnect(self, false);

retry:
		status = pbs_statjob(self->connection_fd, job_id, attrib, NULL);

		fsd_log_info(( "pbs_statjob( fd=%d, job_id=%s, attribs={...} ) =%p", self->connection_fd, job_id, (void*)status));

		if(status == NULL)
		 {
			if (IS_MISSING_JOB)
			 {
				fsd_log_info(( "missing job = %s (code=%d)", job_id, pbs_errno ));
			 }
			else if (IS_TRANSIENT_ERROR && first_try)
			 {
				fsd_log_error(( "pbs_statjob failed, pbs_errno = %d", pbs_errno ));
				check_reconnect(self, true);
				first_try = false;
				goto retry;
			 }
			else
			 {
				pbsdrmaa_exc_raise_pbs( "pbs_statjob", self->connection_fd);
			 }
		 }
	 }
	EXCEPT_DEFAULT
	 {
		if( status != NULL )
			pbs_statfree( status );

		fsd_exc_reraise();
	 }
	FINALLY
	 {
		if(conn_lock)
			conn_lock = fsd_mutex_unlock(&self->session->drm_connection_mutex);
	 }
	END_TRY


	fsd_log_return((""));

	return status;
}

void 
pbsdrmaa_pbs_statjob_free( pbsdrmaa_pbs_conn_t *self, struct batch_status* job_status )
{
	fsd_log_enter((""));

	pbs_statfree( job_status );
}

void 
pbsdrmaa_pbs_sigjob( pbsdrmaa_pbs_conn_t *self, char *job_id, char *signal_name )
{
	int rc = PBSE_NONE;
	volatile bool first_try = true;
	volatile bool conn_lock = false;


	fsd_log_enter((""));

	TRY
	 {
		conn_lock = fsd_mutex_lock(&self->session->drm_connection_mutex);

		check_reconnect(self, false);

retry:
		rc = pbs_sigjob(self->connection_fd, job_id, signal_name, NULL);

		fsd_log_info(( "pbs_sigjob( fd=%d, job_id=%s, signal_name=%s) = %d", self->connection_fd, job_id, signal_name, rc));

		if(rc != PBSE_NONE)
		 {
			fsd_log_error(( "pbs_sigjob failed, pbs_errno = %d", pbs_errno ));
			if (IS_TRANSIENT_ERROR && first_try)
			 {
				check_reconnect(self, true);
				first_try = false;
				goto retry;
			 }
			else
			 {
				pbsdrmaa_exc_raise_pbs( "pbs_sigjob", self->connection_fd);
			 }
		 }
	 }
	EXCEPT_DEFAULT
	 {
		fsd_exc_reraise();
	 }
	FINALLY
	 {
		if(conn_lock)
			conn_lock = fsd_mutex_unlock(&self->session->drm_connection_mutex);
	 }
	END_TRY


	fsd_log_return((""));

}

void 
pbsdrmaa_pbs_deljob( pbsdrmaa_pbs_conn_t *self, char *job_id )
{
	int rc = PBSE_NONE;
	volatile bool first_try = true;
	volatile bool conn_lock = false;


	fsd_log_enter((""));

	TRY
	 {
		conn_lock = fsd_mutex_lock(&self->session->drm_connection_mutex);

		check_reconnect(self, false);

retry:
		rc = pbs_deljob(self->connection_fd, job_id, NULL);

		fsd_log_info(( "pbs_deljob( fd=%d, job_id=%s) = %d", self->connection_fd, job_id, rc));

		if(rc != PBSE_NONE)
		 {
			fsd_log_error(( "pbs_deljob failed, rc = %d, pbs_errno = %d", rc, pbs_errno ));
			if (IS_TRANSIENT_ERROR && first_try)
			 {
				check_reconnect(self, true);
				first_try = false;
				goto retry;
			 }
			else
			 {
				pbsdrmaa_exc_raise_pbs( "pbs_deljob", self->connection_fd);
			 }
		 }
	 }
	EXCEPT_DEFAULT
	 {
		fsd_exc_reraise();
	 }
	FINALLY
	 {
		if(conn_lock)
			conn_lock = fsd_mutex_unlock(&self->session->drm_connection_mutex);
	 }
	END_TRY


	fsd_log_return((""));
}

void 
pbsdrmaa_pbs_rlsjob( pbsdrmaa_pbs_conn_t *self, char *job_id )
{
	int rc = PBSE_NONE;
	volatile bool first_try = true;
	volatile bool conn_lock = false;


	fsd_log_enter((""));

	TRY
	 {
		conn_lock = fsd_mutex_lock(&self->session->drm_connection_mutex);

		check_reconnect(self, false);

retry:
		rc = pbs_rlsjob(self->connection_fd, job_id, USER_HOLD, NULL);

		fsd_log_info(( "pbs_rlsjob( fd=%d, job_id=%s) = %d", self->connection_fd, job_id, rc));

		if(rc != PBSE_NONE)
		 {
			fsd_log_error(( "pbs_rlsjob failed, rc = %d, pbs_errno = %d", rc,  pbs_errno ));
			if (IS_TRANSIENT_ERROR && first_try)
			 {
				check_reconnect(self, true);
				first_try = false;
				goto retry;
			 }
			else
			 {
				pbsdrmaa_exc_raise_pbs( "pbs_rlsjob", self->connection_fd);
			 }
		 }
	 }
	EXCEPT_DEFAULT
	 {
		fsd_exc_reraise();
	 }
	FINALLY
	 {
		if(conn_lock)
			conn_lock = fsd_mutex_unlock(&self->session->drm_connection_mutex);
	 }
	END_TRY


	fsd_log_return((""));
}

void 
pbsdrmaa_pbs_holdjob( pbsdrmaa_pbs_conn_t *self,  char *job_id )
{
	int rc = PBSE_NONE;
	volatile bool first_try = true;
	volatile bool conn_lock = false;


	fsd_log_enter((""));

	TRY
	 {
		conn_lock = fsd_mutex_lock(&self->session->drm_connection_mutex);

		check_reconnect(self, false);

retry:
		rc = pbs_holdjob(self->connection_fd, job_id, USER_HOLD, NULL);

		fsd_log_info(( "pbs_holdjob( fd=%d, job_id=%s) = %d", self->connection_fd, job_id, rc));

		if(rc != PBSE_NONE)
		 {
			fsd_log_error(( "pbs_holdjob failed, rc = %d, pbs_errno = %d", rc, pbs_errno ));
			if (IS_TRANSIENT_ERROR && first_try)
			 {
				check_reconnect(self, true);
				first_try = false;
				goto retry;
			 }
			else
			 {
				pbsdrmaa_exc_raise_pbs( "pbs_holdjob", self->connection_fd);
			 }
		 }
	 }
	EXCEPT_DEFAULT
	 {
		fsd_exc_reraise();
	 }
	FINALLY
	 {
		if(conn_lock)
			conn_lock = fsd_mutex_unlock(&self->session->drm_connection_mutex);
	 }
	END_TRY


	fsd_log_return((""));
}

void 
check_reconnect( pbsdrmaa_pbs_conn_t *self, bool force_reconnect)
{
	int tries_left = ((pbsdrmaa_session_t *)self->session)->max_retries_count;
	int sleep_time = 1;

	fsd_log_enter(("(%d)", self->connection_fd));

	if ( self->connection_fd != -1 ) 
	  {
		if (!force_reconnect)
		  {
			fsd_log_return(("(%d)", self->connection_fd));
			return;
		  }
		else
		 {
			stop_autoclose_thread(self);
			pbs_disconnect(self->connection_fd);
			self->connection_fd = -1;
		 }
	  }



retry_connect: /* Life... */
	self->connection_fd = pbs_connect( self->server );
	fsd_log_info(( "pbs_connect(%s) =%d", self->server, self->connection_fd ));
	if( self->connection_fd < 0 && tries_left-- )
	  {
		sleep(sleep_time);
		sleep_time *=2;
		goto retry_connect;
	  }
	
	if( self->connection_fd < 0 )
		pbsdrmaa_exc_raise_pbs( "pbs_connect", self->connection_fd );
	
	fsd_log_return(("(%d)", self->connection_fd));
}


static void start_autoclose_thread( pbsdrmaa_pbs_conn_t *self )
{


}

static void stop_autoclose_thread( pbsdrmaa_pbs_conn_t *self )
{


}

