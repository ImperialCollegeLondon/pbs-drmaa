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

#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
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
#include <pbs_drmaa/pbs_attrib.h>

#include <errno.h>

enum pbsdrmaa_field_id
{
	PBSDRMAA_FLD_ID_DATE = 0,
	PBSDRMAA_FLD_ID_EVENT = 1,
	PBSDRMAA_FLD_ID_SRC = 2,
	PBSDRMAA_FLD_ID_OBJ_TYPE = 3,
	PBSDRMAA_FLD_ID_OBJ_ID = 4,
	PBSDRMAA_FLD_ID_MSG = 5
};


#define PBSDRMAA_FLD_MSG_0008 "0008"
#define PBSDRMAA_FLD_MSG_0010 "0010"

enum pbsdrmaa_event_type
{
	pbsdrmaa_event_0008 = 8,
	pbsdrmaa_event_0010 = 10
};

static void pbsdrmaa_read_log();

static void pbsdrmaa_select_file( pbsdrmaa_log_reader_t * self);

static void pbsdrmaa_close_log( pbsdrmaa_log_reader_t * self);

static void pbsdrmaa_reopen_log( pbsdrmaa_log_reader_t * self);

static time_t pbsdrmaa_parse_log_timestamp(const char *timestamp, char *unixtime_str, size_t size);

static char *pbsdrmaa_get_exec_host_from_accountig(pbsdrmaa_log_reader_t * log_reader, const char *job_id);

/*
 * Snippets from log files
 *
 * PBS Pro
 *
10/11/2011 14:43:29;0008;Server@nova;Job;2127218.nova;Job Queued at request of mamonski@endor.wcss.wroc.pl, owner = mamonski@endor.wcss.wroc.pl, job name = STDIN, queue = normal
10/11/2011 14:43:31;0008;Server@nova;Job;2127218.nova;Job Modified at request of Scheduler@nova.wcss.wroc.pl
10/11/2011 14:43:31;0008;Server@nova;Job;2127218.nova;Job Run at request of Scheduler@nova.wcss.wroc.pl on exec_vnode (wn698:ncpus=3:mem=2048000kb)+(wn700:ncpus=3:mem=2048000kb)
10/11/2011 14:43:31;0008;Server@nova;Job;2127218.nova;Job Modified at request of Scheduler@nova.wcss.wroc.pl
10/11/2011 14:43:32;0010;Server@nova;Job;2127218.nova;Exit_status=0 resources_used.cpupercent=0 resources_used.cput=00:00:00 resources_used.mem=1768kb resources_used.ncpus=6 resources_used.vmem=19228kb resources_used.walltime=00:00:01

 *
 * Torque
 *
10/11/2011 14:47:59;0008;PBS_Server;Job;15545337.batch.grid.cyf-kr.edu.pl;Job Queued at request of plgmamonski@ui.cyf-kr.edu.pl, owner = plgmamonski@ui.cyf-kr.edu.pl, job name = STDIN, queue = l_short
10/11/2011 14:48:23;0008;PBS_Server;Job;15545337.batch.grid.cyf-kr.edu.pl;Job Run at request of root@batch.grid.cyf-kr.edu.pl
10/11/2011 14:48:24;0010;PBS_Server;Job;15545337.batch.grid.cyf-kr.edu.pl;Exit_status=0 resources_used.cput=00:00:00 resources_used.mem=720kb resources_used.vmem=13308kb resources_used.walltime=00:00:00

deleting job:
I . PBS Pro
a) in Q state
10/16/2011 09:49:25;0008;Server@grass1;Job;2178.grass1.man.poznan.pl;Job Queued at request of mmamonski@grass1.man.poznan.pl, owner = mmamonski@grass1.man.poznan.pl, job name = STDIN, queue = workq
10/16/2011 09:49:25;0008;Server@grass1;Job;2178.grass1.man.poznan.pl;Job Modified at request of Scheduler@grass1.man.poznan.pl
10/16/2011 09:49:37;0008;Server@grass1;Job;2178.grass1.man.poznan.pl;Job to be deleted at request of mmamonski@grass1.man.poznan.pl
10/16/2011 09:49:37;0100;Server@grass1;Job;2178.grass1.man.poznan.pl;dequeuing from workq, state 5


b) in R state
10/16/2011 09:45:12;0080;Server@grass1;Job;2177.grass1.man.poznan.pl;delete job request received
10/16/2011 09:45:12;0008;Server@grass1;Job;2177.grass1.man.poznan.pl;Job sent signal TermJob on delete
10/16/2011 09:45:12;0008;Server@grass1;Job;2177.grass1.man.poznan.pl;Job to be deleted at request of mmamonski@grass1.man.poznan.pl
10/16/2011 09:45:12;0010;Server@grass1;Job;2177.grass1.man.poznan.pl;Exit_status=271 resources_used.cpupercent=0 resources_used.cput=00:00:00 resources_used.mem=2772kb resources_used.ncpus=1 resources_used.vmem=199288kb resources_used.walltime=00:00:26
10/16/2011 09:45:12;0100;Server@grass1;Job;2177.grass1.man.poznan.pl;dequeuing from workq, state 5

II. Torque
a) in Q state
10/15/2011 21:19:25;0008;PBS_Server;Job;113045.grass1.man.poznan.pl;Job deleted at request of mmamonski@grass1.man.poznan.pl
10/15/2011 21:19:25;0100;PBS_Server;Job;113045.grass1.man.poznan.pl;dequeuing from batch, state EXITING

b) in R state
10/15/2011 21:19:47;0008;PBS_Server;Job;113046.grass1.man.poznan.pl;Job deleted at request of mmamonski@grass1.man.poznan.pl
10/15/2011 21:19:47;0008;PBS_Server;Job;113046.grass1.man.poznan.pl;Job sent signal SIGTERM on delete
10/15/2011 21:19:47;0010;PBS_Server;Job;113046.grass1.man.poznan.pl;Exit_status=271 resources_used.cput=00:00:00 resources_used.mem=0kb resources_used.vmem=0kb resources_used.walltime=00:00:10

Log closed:
10/16/2011 00:00:17;0002;PBS_Server;Svr;Log;Log closed

 */
pbsdrmaa_log_reader_t * 
pbsdrmaa_log_reader_new( fsd_drmaa_session_t *session )
{
	pbsdrmaa_log_reader_t *volatile self = NULL;

	fsd_log_enter((""));

	TRY
	{
		fsd_malloc(self, pbsdrmaa_log_reader_t );
		
		self->session = session;

		self->select_file = pbsdrmaa_select_file;
		self->read_log = pbsdrmaa_read_log;	
		self->close = pbsdrmaa_close_log;
		self->reopen = pbsdrmaa_reopen_log;
		
		self->run_flag = true;
		self->fhandle = NULL;
		self->date_changed = true;
		self->first_open = true;
		self->log_path = NULL;
		self->current_offset = 0;
		
	}
	EXCEPT_DEFAULT
	{
		if( self != NULL)
			fsd_free(self);
			
		fsd_exc_reraise();
	}
	END_TRY

	fsd_log_return((""));

	return self;
}


void
pbsdrmaa_log_reader_destroy ( pbsdrmaa_log_reader_t * self )
{
	fsd_log_enter((""));
	TRY
	{
		if(self != NULL)
		{
			fsd_free(self);	
		}
	}
	EXCEPT_DEFAULT
	{
		fsd_exc_reraise();
	}
	END_TRY
	
	fsd_log_return((""));
}


void
pbsdrmaa_read_log( pbsdrmaa_log_reader_t * self )
{
	fsd_log_enter((""));
	
	fsd_mutex_lock( &self->session->mutex );

	TRY
	 {
		while( self->run_flag )
		 {
			TRY
			{
				char *line = NULL;
				
				self->select_file(self);

				while ((line = fsd_readline(self->fhandle)) != NULL)
				 {
					int field_id = PBSDRMAA_FLD_ID_DATE;
					char *tok_ctx = NULL;
					char *field_token = NULL;
					char *event_timestamp = NULL;
					int event_type = -1;
					fsd_job_t *job = NULL;

					/* at first detect if this not the end of log file */
					if (strstr(line, "Log;Log closed")) /*TODO try to be more effective and safe */
					 {
						fsd_log_debug(("WT - Date changed. Closing log file"));
						self->date_changed = true;
						goto cleanup;
					 }

					for (field_token = strtok_r(line, ";", &tok_ctx); field_token; field_token = strtok_r(NULL, ";", &tok_ctx), field_id++)
					 {
						if ( field_id == PBSDRMAA_FLD_ID_DATE)
						 {
							event_timestamp = field_token;
#ifdef PBS_PBS_PROFESSIONAL
							/*additional check */
							TRY
							{
							 (void)pbsdrmaa_parse_log_timestamp(event_timestamp, timestamp_unix, sizeof(timestamp_unix));
							}
							EXCEPT_DEFAULT
							{
								fsd_log_error(("Failed to parse timestamp: %s. Log corrupted?", event_timestamp));
							}
							END_TRY
#endif
						 }
						else if ( field_id == PBSDRMAA_FLD_ID_EVENT)
						 {
							if (strncmp(field_token, PBSDRMAA_FLD_MSG_0008, 4) == 0)
								event_type = pbsdrmaa_event_0008;
							else if (strncmp(field_token, PBSDRMAA_FLD_MSG_0010, 4) == 0)
								event_type = pbsdrmaa_event_0010;
							else
							 {
								goto cleanup; /*we are interested only in the above log messages */
							 }
						 }
						else if ( field_id == PBSDRMAA_FLD_ID_SRC)
						 {
							/* not used ignore */
						 }
						else if (field_id  == PBSDRMAA_FLD_ID_OBJ_TYPE)
						 {
							if (strncmp(field_token, "Job", 3) != 0)
							 {
								goto cleanup; /* we are interested only in job events */
							 }
						 }
						else if (field_id == PBSDRMAA_FLD_ID_OBJ_ID)
						 {
							const char *event_jobid = field_token;
							
							if (!isdigit(event_jobid[0]))
							 {
								fsd_log_debug(("WT - Invalid job: %s", event_jobid)); 
								goto cleanup;
							 }

							job = self->session->get_job( self->session, event_jobid );

							if( job )
							 {
								fsd_log_debug(("WT - Found job event: %s", event_jobid));
							 }
							else
							 {
								fsd_log_debug(("WT - Unknown job: %s", event_jobid)); /* Not a DRMAA job */
								goto cleanup;
							 }
					 	 }
						else if (field_id == PBSDRMAA_FLD_ID_MSG)
						 {
							char *msg = field_token;
							struct batch_status status;
							struct attrl *attribs = NULL;
							bool in_running_state = false;

							if (event_type == pbsdrmaa_event_0008 && strncmp(msg, "Job Queued", 10) == 0)
							 {
								/* Queued
								 * PBS Pro: 10/11/2011 14:43:29;0008;Server@nova;Job;2127218.nova;Job Queued at request of mamonski@endor.wcss.wroc.pl, owner = mamonski@endor.wcss.wroc.pl, job name = STDIN, queue = normal
								 * Torque:  10/11/2011 14:47:59;0008;PBS_Server;Job;15545337.batch.grid.cyf-kr.edu.pl;Job Queued at request of plgmamonski@ui.cyf-kr.edu.pl, owner = plgmamonski@ui.cyf-kr.edu.pl, job name = STDIN, queue = l_short
								 */
								char *p_queue = NULL;

								fsd_log_info(("WT - Detected queuing of job %s", job->job_id));

								if ((p_queue = strstr(msg,"queue =")) == NULL)
									fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"No queue attribute found in log line = %s", line);

								attribs = pbsdrmaa_add_attr(attribs, PBSDRMAA_JOB_STATE, "Q");
								attribs = pbsdrmaa_add_attr(attribs, PBSDRMAA_QUEUE, p_queue + 7);
							 }
							else if (event_type == pbsdrmaa_event_0008 && strncmp(msg, "Job Run", 7) == 0)
							{
								/*
								 * Running
								 * Torque: 10/11/2011 14:48:23;0008;PBS_Server;Job;15545337.batch.grid.cyf-kr.edu.pl;Job Run at request of root@batch.grid.cyf-kr.edu.pl
								 * PBS Pro: 10/11/2011 14:43:31;0008;Server@nova;Job;2127218.nova;Job Run at request of Scheduler@nova.wcss.wroc.pl on exec_vnode (wn698:ncpus=3:mem=2048000kb)+(wn700:ncpus=3:mem=2048000kb)
								 */
								char timestamp_unix[64];

								fsd_log_info(("WT - Detected start of job %s", job->job_id));

								(void)pbsdrmaa_parse_log_timestamp(event_timestamp, timestamp_unix, sizeof(timestamp_unix));

								in_running_state = true;

								attribs = pbsdrmaa_add_attr(attribs, PBSDRMAA_JOB_STATE, "R");
								attribs = pbsdrmaa_add_attr(attribs, PBSDRMAA_START_TIME, timestamp_unix);
#ifdef PBS_PROFESSIONAL
									{
										char *p_vnode = NULL;
										if ((p_vnode = strstr(msg, "exec_vnode")))
										 {
											attribs = pbsdrmaa_add_attr(attribs, PBSDRMAA_EXECUTION_VNODE, p_vnode + 11);
										 }
									}
#endif
							 }
#ifndef PBS_PROFESSIONAL
							else if (event_type == pbsdrmaa_event_0008 && strncmp(msg, "Job deleted", 11) == 0)
#else
							else if (event_type == pbsdrmaa_event_0008 && strncmp(msg, "Job to be deleted", 17) == 0)
#endif
							 {
							/* Deleted
							 * PBS Pro: 10/16/2011 09:45:12;0008;Server@grass1;Job;2177.grass1.man.poznan.pl;Job to be deleted at request of mmamonski@grass1.man.poznan.pl
							 * Torque: 10/15/2011 21:19:25;0008;PBS_Server;Job;113045.grass1.man.poznan.pl;Job deleted at request of mmamonski@grass1.man.poznan.pl
							 */
								char timestamp_unix[64];

								fsd_log_info(("WT - Detected deletion of job %s", job->job_id));

								(void)pbsdrmaa_parse_log_timestamp(event_timestamp, timestamp_unix, sizeof(timestamp_unix));

								if (job->state < DRMAA_PS_RUNNING)
								 {
									fsd_log_info(("WT - Job %s killed before entering running state (%d).", job->job_id, job->state));

#ifdef PBS_PROFESSIONAL
									attribs = pbsdrmaa_add_attr(attribs, PBSDRMAA_JOB_STATE, "F");
#else
									attribs = pbsdrmaa_add_attr(attribs, PBSDRMAA_JOB_STATE, "C");
#endif

									attribs = pbsdrmaa_add_attr(attribs, PBSDRMAA_MTIME, timestamp_unix);
									attribs = pbsdrmaa_add_attr(attribs, PBSDRMAA_EXIT_STATUS, "-101");
								 }
								else
								 {
									fsd_log_info(("WT - Job %s killed after entering running state (%d). Waiting for Completed event...", job->job_id, job->state));
									goto cleanup; /* job was started, ignore, wait for Exit_status message */
								 }
							 }
							else if (event_type == pbsdrmaa_event_0010 && (strncmp(msg, "Exit_status=", 12) == 0))
							 {
							/* Completed:
							 * PBS Pro: 10/11/2011 14:43:32;0010;Server@nova;Job;2127218.nova;Exit_status=0 resources_used.cpupercent=0 resources_used.cput=00:00:00 resources_used.mem=1768kb resources_used.ncpus=6 resources_used.vmem=19228kb resources_used.walltime=00:00:01
							 * Torque: 10/11/2011 14:48:24;0010;PBS_Server;Job;15545337.batch.grid.cyf-kr.edu.pl;Exit_status=0 resources_used.cput=00:00:00 resources_used.mem=720kb resources_used.vmem=13308kb resources_used.walltime=00:00:00
							 */
								char timestamp_unix[64];
								time_t timestamp_time_t = pbsdrmaa_parse_log_timestamp(event_timestamp, timestamp_unix, sizeof(timestamp_unix));
								char *tok_ctx2 = NULL;
								char *token = NULL;

#ifdef PBS_PBS_PROFESSIONAL
								attribs = pbsdrmaa_add_attr(attribs, PBSDRMAA_JOB_STATE, "F");
#else
								attribs = pbsdrmaa_add_attr(attribs, PBSDRMAA_JOB_STATE, "C");
#endif
								attribs = pbsdrmaa_add_attr(attribs, PBSDRMAA_MTIME, timestamp_unix);

								/* tokenize !!! */
								for (token = strtok_r(msg, " ", &tok_ctx2); token; token = strtok_r(NULL, " ", &tok_ctx2))
								 {
									if (strncmp(token, "Exit_status=", 12) == 0)
									 {
										token[11] = '\0';
										attribs = pbsdrmaa_add_attr(attribs, PBSDRMAA_EXIT_STATUS, token + 12);
										fsd_log_info(("WT - Completion of job %s (Exit_status=%s) detected after %d seconds", job->job_id, token+12, (int)(time(NULL) - timestamp_time_t) ));
									 }
									else if (strncmp(token, "resources_used.cput=", 20) == 0)
									 {
										token[19] = '\0';
										attribs = pbsdrmaa_add_attr(attribs, token, token + 20);
									 }
									else if (strncmp(token, "resources_used.mem=", 19) == 0)
									 {
										token[18] = '\0';
										attribs = pbsdrmaa_add_attr(attribs, token, token + 19);
									 }
									else if (strncmp(token, "resources_used.vmem=", 20) == 0)
									 {
										token[19] = '\0';
										attribs = pbsdrmaa_add_attr(attribs, token, token + 20);
									 }
									else if (strncmp(token, "resources_used.walltime=", 24) == 0)
									 {
										token[23] = '\0';
										attribs = pbsdrmaa_add_attr(attribs, token, token + 24);
									 }
								 }

								if (!job->execution_hosts)
								 {
									char *exec_host = NULL;
									fsd_log_info(("WT - No execution host information for job %s. Reading accounting logs...", job->job_id));
									exec_host = pbsdrmaa_get_exec_host_from_accountig(self, job->job_id);
									if (exec_host)
									 {
										attribs = pbsdrmaa_add_attr(attribs, PBSDRMAA_EXECUTION_HOST, exec_host);
										fsd_free(exec_host);
									 }
								 }
							 }
							else
							{
								fsd_log_debug(("Ignoring msg(type=%d) = %s", event_type,  msg));
								goto cleanup; /* ignore other job events*/
							}
					
							fsd_log_debug(("WT - updating job: %s", job->job_id ));
							status.name = job->job_id;
							status.attribs = attribs;

							((pbsdrmaa_job_t *)job)->update( job, &status );

							if ( in_running_state )
							 {
								fsd_log_debug(("WT - forcing update of job: %s", job->job_id ));
								TRY
								{
									job->update_status( job );
								}
								EXCEPT_DEFAULT
								{
									/*TODO: distinguish between invalid job and internal errors */
									fsd_log_debug(("Job finished just after entering running state: %s", job->job_id));
								}
								END_TRY
							 }


							pbsdrmaa_free_attrl(attribs); /* TODO free on exception */

							fsd_cond_broadcast( &job->status_cond);
							fsd_cond_broadcast( &self->session->wait_condition );

						 }
						else
						 {
							fsd_assert(0); /*not reached */
						 }
					 }
				cleanup:
					fsd_free(line); /* TODO what about exceptions */		
					if ( job )
						job->release( job );



				 } /* end of while getline loop */



				fsd_mutex_unlock( &self->session->mutex );

				/* close */
				self->close(self);

				sleep(((pbsdrmaa_session_t *)self->session)->wait_thread_sleep_time);

				/* and reopen log file */
				self->reopen(self);

				fsd_mutex_lock( &self->session->mutex );

				self->run_flag = self->session->wait_thread_run_flag;
			}
			EXCEPT_DEFAULT
			{
				const fsd_exc_t *e = fsd_exc_get();
				/* Its better to exit and communicate error rather then let the application to hang */
				fsd_log_fatal(( "Exception in wait thread: <%d:%s>. Exiting !!!", e->code(e), e->message(e) ));
				exit(1);
			}
			END_TRY
		 }

		if(self->fhandle)
			fclose(self->fhandle);

		fsd_log_debug(("WT - Log file closed"));
	}
	FINALLY
	{
	 	fsd_log_debug(("WT - Terminated."));
		fsd_mutex_unlock( &self->session->mutex ); /**/
	}
	END_TRY
	
	fsd_log_return((""));
}

void
pbsdrmaa_select_file( pbsdrmaa_log_reader_t * self )
{
	pbsdrmaa_session_t *pbssession = (pbsdrmaa_session_t*) self->session;
	
	if (self->date_changed)
	 {
		int num_tries = 0;
		struct tm tm; 
		char *old_log_path = NULL;
		
		fsd_log_enter((""));
		
		if(!self->first_open)
			time(&self->t);	
		else
			self->t = pbssession->log_file_initial_time;
			
		localtime_r(&self->t,&tm);
				
		#define DRMAA_WAIT_THREAD_MAX_TRIES (12)
		/* generate new date, close file and open new */
		old_log_path = self->log_path;

		self->log_path = fsd_asprintf("%s/server_logs/%04d%02d%02d", pbssession->pbs_home, tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);

		if(self->fhandle)
			fclose(self->fhandle);

		fsd_log_info(("Opening log file: %s",self->log_path));
				
	retry:
		if ((self->fhandle = fopen(self->log_path,"r")) == NULL && (num_tries > DRMAA_WAIT_THREAD_MAX_TRIES || self->first_open))
		 {
			fsd_log_error(("Can't open log file: %s. Verify pbs_home. Running standard wait_thread.", self->log_path));
			fsd_log_error(("Remember that without keep_completed set the standard wait_thread won't provide information about job exit status"));
			/*pbssession->super.enable_wait_thread = false;*/ /* run not wait_thread */
			pbssession->wait_thread_log = false;
			pbssession->super.wait_thread = pbssession->super_wait_thread;
			pbssession->super.wait_thread(self->session);
		 }
		else if ( self->fhandle == NULL )
		 { /* Torque seems not to create a new file immediately after the old one is closed */
			fsd_log_warning(("Can't open log file: %s. Retries count: %d", self->log_path, num_tries));
			num_tries++;
			sleep(2 * num_tries);
			goto retry;
		 }

		fsd_log_debug(("Log file opened"));

		if(self->first_open)
		 {
			fsd_log_debug(("Log file lseek"));

			if(fseek(self->fhandle, pbssession->log_file_initial_size, SEEK_SET) == (off_t) -1)
			 {
				fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"fseek error");
			 }
			self->first_open = false;
		 }
		else if (old_log_path && strcmp(old_log_path, self->log_path) == 0)
		 {
			fsd_log_info(("PBS restarted. Seeking log file %u", (unsigned int)self->current_offset));
			if(fseek(self->fhandle, self->current_offset, SEEK_SET) == (off_t) -1)
			 {
				fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"fseek error");
			 }
		 }

		self->date_changed = false;
		
		fsd_free(old_log_path);

		fsd_log_return((""));
	}	
}

time_t
pbsdrmaa_parse_log_timestamp(const char *timestamp, char *unixtime_str, size_t size)
{
	struct tm temp_time_tm;
	memset(&temp_time_tm, 0, sizeof(temp_time_tm));
	temp_time_tm.tm_isdst = -1;

	if (strptime(timestamp, "%m/%d/%Y %H:%M:%S", &temp_time_tm) == NULL)
	 {
		fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"WT - failed to parse log timestamp: %s", timestamp);
	 }
	else
	 {
		time_t temp_time = mktime(&temp_time_tm);
		snprintf(unixtime_str, size, "%lu", temp_time);
		return temp_time;
	 }
}

char *
pbsdrmaa_get_exec_host_from_accountig(pbsdrmaa_log_reader_t * log_reader, const char *job_id)
{
		pbsdrmaa_session_t *pbssession = (pbsdrmaa_session_t*) log_reader->session;
		struct tm tm;
		time_t tm_t;
		char *line = NULL;
		char *exec_host = NULL;
		char *log_path = NULL;
		FILE *fhandle = NULL;

		fsd_log_enter(("(job_id=%s)", job_id));

		tm_t = time(NULL);
		localtime_r(&tm_t, &tm);

		log_path = fsd_asprintf("%s/server_priv/accounting/%04d%02d%02d", pbssession->pbs_home, tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);

		fsd_log_info(("Opening accounting log file: %s", log_path));

		if ((fhandle = fopen(log_path, "r")) == NULL)
		 {
			fsd_log_error(("Failed to open accounting log file: %s", log_path));
			fsd_free(log_path);
			return NULL;
		 }

		fsd_free(log_path);
/*
10/27/2011 14:09:32;E;114249.grass1.man.poznan.pl;user=drmaa group=drmaa jobname=none queue=shortq ctime=1319717371 qtime=1319717371 etime=1319717371 start=1319717372 owner=drmaa@grass1.man.poznan.pl exec_host=grass4.man.poznan.pl/0 Resource_List.neednodes=1 Resource_List.nodect=1 Resource_List.nodes=1 Resource_List.walltime=02:00:00 session=28561 end=1319717372 Exit_status=0 resources_used.cput=00:00:00 resources_used.mem=0kb resources_used.vmem=0kb resources_used.walltime=00:00:00
 */
		while ((line = fsd_readline(fhandle)) != NULL)
		 {

			if (line[20] == 'E'  && strncmp(line + 22, job_id, strlen(job_id)) == 0 )
			 {
				char *p = NULL;

				fsd_log_debug(("Matched accounting log record = %s", line));

				if (!(exec_host = strstr(line, "exec_host")))
				 {
					fsd_log_error(("Invalid accounting record: %s", exec_host));
					break;
				 }

				exec_host += 10;

				p = exec_host;
				while (*p != ' ' && *p != '\0')
					p++;
				*p = '\0';

				break;
			 }

			fsd_free(line);
		 }

		if (exec_host)
		 {
			fsd_log_info(("Job %s was executing on hosts %s.", job_id, exec_host));
			exec_host = fsd_strdup(exec_host);
		 }
		else
		 {
			fsd_log_error(("Could not find executions hosts for %s.", job_id));
		 }

		if (line)
			fsd_free(line);

		fclose(fhandle);

		return exec_host;
}

void
pbsdrmaa_close_log( pbsdrmaa_log_reader_t * self )
{

	self->current_offset = ftello(self->fhandle);
	
	fsd_log_debug(("Closing log  file (offset=%d)", (int)self->current_offset));

	fclose(self->fhandle);

	self->fhandle = NULL;
}

void
pbsdrmaa_reopen_log( pbsdrmaa_log_reader_t * self )
{
	fsd_log_debug(("Reopening log file: %s (offset=%d)", self->log_path, (int)self->current_offset));

	if ((self->fhandle = fopen(self->log_path,"r")) == NULL)
	 {
		fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"Failed to reopen log file");
	 }

	if(fseek(self->fhandle, self->current_offset, SEEK_SET) == (off_t) -1)
	 {
		fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"fseek error");
	 }
}

