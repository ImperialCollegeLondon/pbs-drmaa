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
#include <drmaa_utils/session.h>
#include <drmaa_utils/datetime.h>

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

static void pbsdrmaa_pbs_reconnect_internal( pbsdrmaa_pbs_conn_t *self, bool reconnect);
	
pbsdrmaa_pbs_conn_t * 
pbsdrmaa_pbs_conn_new( pbsdrmaa_session_t *session, char *server )
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
		self->last_usage = time(NULL);
	        
		/*ignore SIGPIPE - otheriwse pbs_disconnect cause the program to exit */
		signal(SIGPIPE, SIG_IGN);	

		pbsdrmaa_pbs_reconnect_internal(self, false);
	  }
	EXCEPT_DEFAULT
	  {
		if( self != NULL)
		  {
			fsd_free(self->server);
			fsd_free(self);

			if (self->connection_fd != -1)
				pbs_disconnect(self->connection_fd);
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


}

struct batch_status* 
pbsdrmaa_pbs_statjob( pbsdrmaa_pbs_conn_t *self,  char *job_id, struct attrl *attrib )
{

}

void 
pbsdrmaa_pbs_statjob_free( pbsdrmaa_pbs_conn_t *self, struct batch_status* job_status )
{


}

void 
pbsdrmaa_pbs_sigjob( pbsdrmaa_pbs_conn_t *self, char *job_id, char *signal )
{


}

void 
pbsdrmaa_pbs_deljob( pbsdrmaa_pbs_conn_t *self, char *job_id )
{

}

void 
pbsdrmaa_pbs_rlsjob( pbsdrmaa_pbs_conn_t *self, char *job_id )
{


}

void 
pbsdrmaa_pbs_holdjob( pbsdrmaa_pbs_conn_t *self,  char *job_id )
{

}

void 
pbsdrmaa_pbs_reconnect_internal( pbsdrmaa_pbs_conn_t *self, bool force_reconnect)
{
	int tries_left = self->session->max_retries_count;
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
		pbsdrmaa_exc_raise_pbs( "pbs_connect" );
	
	fsd_log_return(("(%d)", self->connection_fd));
}

