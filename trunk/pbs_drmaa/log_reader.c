/* $Id: log_reader.c 323 2010-09-21 21:31:29Z mmatloka $ */
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
#include <string.h>
#include <unistd.h>
#include <sys/select.h>
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

#include <errno.h>

static void
pbsdrmaa_read_log();

static void
pbsdrmaa_select_file_wait_thread ( pbsdrmaa_log_reader_t * self);

static ssize_t
pbsdrmaa_read_line_wait_thread ( pbsdrmaa_log_reader_t * self, char * line, char * buffer, ssize_t size, int * idx, int * end_idx, int * line_idx );

static void
pbsdrmaa_select_file_job_on_missing ( pbsdrmaa_log_reader_t * self );

static ssize_t
pbsdrmaa_read_line_job_on_missing ( pbsdrmaa_log_reader_t * self, char * line, char * buffer, ssize_t size, int * idx, int * end_idx, int * line_idx );

int 
fsd_job_id_cmp(const char *s1, const char *s2);

int 
pbsdrmaa_date_compare(const void *a, const void *b) ;

pbsdrmaa_log_reader_t * 
pbsdrmaa_log_reader_new ( fsd_drmaa_session_t *session, fsd_job_t *job )
{
	pbsdrmaa_log_reader_t *volatile self = NULL;

	fsd_log_enter((""));
	TRY
	{
		fsd_malloc(self, pbsdrmaa_log_reader_t );
		
		self->session = session;
		
		/* ~templete method pattern */
		if(job != NULL) /* job on missing */
		{
			self->job = job;
			self->name = "Job_on_missing";
			self->select_file = pbsdrmaa_select_file_job_on_missing;
			self->read_line = pbsdrmaa_read_line_job_on_missing;
		}
		else /* wait thread */
		{
			self->job = NULL;
			self->name = "WT";
			self->select_file = pbsdrmaa_select_file_wait_thread;
			self->read_line = pbsdrmaa_read_line_wait_thread;
		}		
		self->read_log = pbsdrmaa_read_log;	
		
		self->log_files = NULL;
		self->log_files_number = 0;
		
		self->run_flag = true;
		self->fd = -1;
		self->date_changed = true;
		self->first_open = true;
		
		self->log_file_initial_size = 0;
		self->log_file_read_size = 0;
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
			int i = -1;
			for(i = 0; i < self->log_files_number ; i++)
				fsd_free(self->log_files[i]);
			fsd_free(self->log_files);
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

void 
pbsdrmaa_read_log( pbsdrmaa_log_reader_t * self )
{
	pbsdrmaa_job_t *pbsjob = (pbsdrmaa_job_t*) self->job;	
	fsd_job_t *volatile temp_job = NULL;
		
	fsd_log_enter((""));
	
	if(self->job == NULL)
		fsd_mutex_lock( &self->session->mutex );

	TRY
	{		
		while( self->run_flag )
		TRY
		{
			char line[4096] = "";
			char buffer[4096] = "";
			int idx = 0, end_idx = 0, line_idx = 0;
			
			self->select_file(self);

			while ((self->read_line(self, line,buffer, sizeof(line), &idx,&end_idx,&line_idx)) > 0) 			
			{
				const char *volatile ptr = line;
  				char field[256] = "";
				char job_id[256] = "";
				char event[256] = "";
				int volatile field_n = 0;
 				int n;
				
				bool volatile job_id_match = false;
				bool volatile event_match = false;
				bool volatile log_event = false;
				bool volatile log_match = false;
				bool volatile older_job_found = false;
				bool volatile job_found = false;
  				char *  temp_date = NULL;
				
				struct batch_status status;
				status.next = NULL;

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
							fsd_log_error(("%s - strlcpy error",self->name));
						}
						event_match = true;									
					}
					else if(event_match && field_n == FLD_ID)
					{	
						TRY
						{	
							if(self->job == NULL) /* wait_thread */
							{
								temp_job = self->session->get_job( self->session, field );
								pbsjob = (pbsdrmaa_job_t*) temp_job;

								if( temp_job )
								{
									if(strlcpy(job_id, field,sizeof(job_id)) > sizeof(job_id)) {
										fsd_log_error(("%s - strlcpy error",self->name));
									}
									fsd_log_debug(("%s - job_id: %s",self->name,job_id));
									status.name = fsd_strdup(job_id);
									job_id_match = true; /* job_id is in drmaa */	
								}
								else 
								{
									fsd_log_debug(("%s - Unknown job: %s", self->name,field));
								}
							}
							else /* job_on_missing */
							{
								int diff = -1;
								diff = fsd_job_id_cmp(self->job->job_id,field);
								if( diff == 0)
								{
									/* read this file to the place we started and exit*/
									fsd_log_debug(("Job_on_missing found job: %s",self->job->job_id));
									job_found = true;
									older_job_found = false;
									self->run_flag = false;
									job_id_match = true; 
									status.name = fsd_strdup(self->job->job_id);									
								}
								else if ( !job_found && diff >= 1)
								{
									/* older job, find its beginning */
									fsd_log_debug(("Job_on_missing found older job than %s : %s",self->job->job_id,field));
									older_job_found = true;
									job_id_match = true; 
									status.name = fsd_strdup(self->job->job_id);
								}
								else  if( !job_found )
								{
									fsd_log_debug(("Job_on_missing found newer job than %s : %s",self->job->job_id,field));
								}								
							}
						}
						END_TRY	
					}
					else if(job_id_match && field_n == FLD_MSG)
					{						
						/* parse msg - depends on FLD_EVENT */
						struct attrl struct_resource_cput,struct_resource_mem,struct_resource_vmem,
							struct_resource_walltime, struct_status, struct_state, struct_start_time,struct_mtime, struct_queue, struct_account_name;	
						
						bool state_running = false;

						memset(&struct_status,0,sizeof(struct attrl)); /**/
						memset(&struct_state,0,sizeof(struct attrl));
						memset(&struct_resource_cput,0,sizeof(struct attrl));
						memset(&struct_resource_mem,0,sizeof(struct attrl));
						memset(&struct_resource_vmem,0,sizeof(struct attrl));
						memset(&struct_resource_walltime,0,sizeof(struct attrl));
						memset(&struct_start_time,0,sizeof(struct attrl));
						memset(&struct_mtime,0,sizeof(struct attrl));
						memset(&struct_queue,0,sizeof(struct attrl));
						memset(&struct_account_name,0,sizeof(struct attrl));
								
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
								if(older_job_found) /* job_on_missing - older job beginning - read this file and end */
								{
									self->run_flag = false;
									fsd_log_debug(("Job_on_missing found older job beginning"));
									fsd_free(status.name);
									break;
								}
							}		
							if(field[4] == 'M') { /* modified */
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
									fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"%s - Memory allocation wasn't possible",self->name);
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
						 
						if(self->job == NULL) /* wait_thread */
						{ 
							if ( state_running )
							{
								fsd_log_debug(("WT - forcing update of job: %s", temp_job->job_id ));
								temp_job->update_status( temp_job );
							}
							else
							{
								fsd_log_debug(("%s - updating job: %s",self->name, temp_job->job_id ));							
								pbsjob->update( temp_job, &status );
							}
						 }
						 else if( job_found ) /* job_on_missing */
						 {
							fsd_log_debug(("Job_on_missing - updating job: %s", self->job->job_id ));							
							pbsjob->update( self->job, &status );
						 }
						
						if(self->job == NULL)
						{
							fsd_cond_broadcast( &temp_job->status_cond);
							fsd_cond_broadcast( &self->session->wait_condition );
						}
						if ( temp_job )
							temp_job->release( temp_job );
	
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
					else if( self->job == NULL && log_match && field_n == FLD_MSG && strncmp(field,"Log closed",10) == 0) 
					{
						fsd_log_debug(("%s - Date changed. Closing log file",self->name));
						self->date_changed = true;
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

				fsd_free(temp_date);			
			} /* end of while getline loop */			
			
			if(self->job == NULL)
			{
				fsd_mutex_unlock( &self->session->mutex );			
				usleep(1000000);
				fsd_mutex_lock( &self->session->mutex );	
			}
			
			if(self->job == NULL)
			{
				self->run_flag = self->session->wait_thread_run_flag;
			}
		}		
		EXCEPT_DEFAULT
		 {
			const fsd_exc_t *e = fsd_exc_get();

			fsd_log_error(( "%s: <%d:%s>", self->name, e->code(e), e->message(e) ));
			fsd_exc_reraise();
		 }
		END_TRY

		if(self->fd != -1)
			close(self->fd);
		fsd_log_debug(("%s - Log file closed",self->name));	
	}
	FINALLY
	{
	 	fsd_log_debug(("%s - Terminated.",self->name));	
		if(self->job == NULL)
			fsd_mutex_unlock( &self->session->mutex ); /**/
	}
	END_TRY
	
	fsd_log_return((""));
}

void
pbsdrmaa_select_file_wait_thread ( pbsdrmaa_log_reader_t * self )
{
	pbsdrmaa_session_t *pbssession = (pbsdrmaa_session_t*) self->session;
	
	if(self->date_changed)
	{
		char * log_path = NULL;
		int num_tries = 0;
		struct tm tm; 
		
		fsd_log_enter((""));
		
		if(!self->first_open)
			time(&self->t);	
		else
			self->t = pbssession->log_file_initial_time;
			
		localtime_r(&self->t,&tm);
				
		#define DRMAA_WAIT_THREAD_MAX_TRIES (12)
		/* generate new date, close file and open new */
		if((log_path = fsd_asprintf("%s/server_logs/%04d%02d%02d",
					pbssession->pbs_home,	 
					tm.tm_year + 1900,
					tm.tm_mon + 1,
					tm.tm_mday)) == NULL) {
			fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"WT - Memory allocation wasn't possible");
		}

		if(self->fd != -1)
			close(self->fd);

		fsd_log_debug(("Log file: %s",log_path));
				
	retry:
		if((self->fd = open(log_path,O_RDONLY) ) == -1 && num_tries > DRMAA_WAIT_THREAD_MAX_TRIES )
		{
			fsd_log_error(("Can't open log file. Verify pbs_home. Running standard wait_thread."));
			fsd_log_error(("Remember that without keep_completed set standard wait_thread won't run correctly"));
			/*pbssession->super.enable_wait_thread = false;*/ /* run not wait_thread */
			pbssession->wait_thread_log = false;
			pbssession->super.wait_thread = pbssession->super_wait_thread;
			pbssession->super.wait_thread(self->session);
		} else if ( self->fd == -1 ) {
			fsd_log_warning(("Can't open log file: %s. Retries count: %d", log_path, num_tries));
			num_tries++;
			sleep(5);
			goto retry;
		}

		fsd_free(log_path);

		fsd_log_debug(("Log file opened"));

		if(self->first_open) {
			fsd_log_debug(("Log file lseek"));
			if(lseek(self->fd,pbssession->log_file_initial_size,SEEK_SET) == (off_t) -1) {
				char errbuf[256] = "InternalError";
				(void)strerror_r(errno, errbuf, 256);
				fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"lseek error: %s",errbuf);
			}
			self->first_open = false;
		}

		self->date_changed = false;
		
		fsd_log_return((""));
	}	
}

ssize_t
pbsdrmaa_read_line_wait_thread ( pbsdrmaa_log_reader_t * self, char * line, char * buffer, ssize_t size, int * idx, int * end_idx, int * line_idx )
{
	return fsd_getline_buffered(line,buffer,size,self->fd,idx,end_idx,line_idx);
}

/* reverse date compare*/
int 
pbsdrmaa_date_compare(const void *a, const void *b) 
{
   const char *ia = *(const char **) a;
   const char *ib = *(const char **) b;
   return strcmp(ib, ia);
}

void
pbsdrmaa_select_file_job_on_missing( pbsdrmaa_log_reader_t * self )
{
	pbsdrmaa_session_t *pbssession = (pbsdrmaa_session_t*) self->session;	
	
	char * log_path = NULL;
	int num_tries = 0;
	static int file_number = 0;
	fsd_log_enter((""));
		
	if(self->first_open) 
	{			
		DIR *dp = NULL;		
		char * path = NULL;
		struct dirent *ep = NULL;
		
		if((path = fsd_asprintf("%s/server_logs/",pbssession->pbs_home)) == NULL)
			fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"Job_on_missing - Memory allocation wasn't possible");
		
		self->log_files_number = 0;     
		dp = opendir (path);

		fsd_calloc(self->log_files,2,char*);
  	
		if (dp != NULL)
		{
			while ((ep = readdir (dp)))
			{
				self->log_files_number++;
				if(self->log_files_number > 2)
					fsd_realloc(self->log_files,self->log_files_number,char *);
				
				self->log_files[self->log_files_number-1] = fsd_strdup(ep->d_name);
			}
			(void) closedir (dp);
		}
		else
			fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"Job_on_missing - Couldn't open the directory");

		qsort(self->log_files,self->log_files_number,sizeof(char *),pbsdrmaa_date_compare);
		
		if(self->log_files_number <= 2)
		{
			self->run_flag = false;
			fsd_log_error(("Job_on_missing - No log files available"));
		}
		
		self->first_open = false;
		fsd_free(path);
	}	
	else /* check previous day*/
	{
		if(++file_number > self->log_files_number - 2)
			fsd_log_error(("Job_on_missing - All available log files checked"));
		else
			fsd_log_debug(("Job_on_missing checking previous day"));
		
		self->run_flag = false;
		pbsdrmaa_job_on_missing_standard( self->job );				
	}
	
	#define DRMAA_WAIT_THREAD_MAX_TRIES (12)
	if((log_path = fsd_asprintf("%s/server_logs/%s",
				pbssession->pbs_home,	 
				self->log_files[file_number])) == NULL) {
		fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"Job_on_missing - Memory allocation wasn't possible");
	}

	if(self->fd != -1)
		close(self->fd);

	fsd_log_debug(("Log file: %s",log_path));
				
retry:
	if((self->fd = open(log_path,O_RDONLY) ) == -1 && num_tries > DRMAA_WAIT_THREAD_MAX_TRIES )
	{
		fsd_log_error(("Can't open log file. Verify pbs_home. Running standard job_on_missing"));
		fsd_log_error(("Remember that without keep_completed set standard job_on_missing won't run correctly"));
		self->run_flag = false;
		pbsdrmaa_job_on_missing_standard( self->job );			
	} else if ( self->fd == -1 ) {
		fsd_log_warning(("Can't open log file: %s. Retries count: %d", log_path, num_tries));
		num_tries++;
		sleep(5);
		goto retry;
	}
	else
	{
		struct stat statbuf;
		if(stat(log_path,&statbuf) == -1) {
				char errbuf[256] = "InternalError";
				(void)strerror_r(errno, errbuf, 256);
				fsd_exc_raise_fmt(FSD_ERRNO_INTERNAL_ERROR,"stat error: %s",errbuf);
		}
		self->log_file_read_size = 0;
		self->log_file_initial_size = statbuf.st_size;
		fsd_log_debug(("Set log_file_initial_size %ld",self->log_file_initial_size));
	}

	fsd_free(log_path);

	fsd_log_debug(("Log file opened"));
	
	fsd_log_return((""));
}

ssize_t
pbsdrmaa_read_line_job_on_missing ( pbsdrmaa_log_reader_t * self, char * line, char * buffer, ssize_t size, int * idx, int * end_idx, int * line_idx )
{
	int n = fsd_getline_buffered(line,buffer,size,self->fd, idx, end_idx, line_idx);
	
	if(n >= 0)
		self->log_file_read_size += n;
		
	if(self->log_file_read_size >= self->log_file_initial_size)
		return -1; 

	return n; 
}

int 
fsd_job_id_cmp(const char *s1, const char *s2) /* maybe move to drmaa_utils? */
{
	int job1;
	int job2;
	char *rest = NULL;
	char *token = NULL;
	char *ptr = fsd_strdup(s1);
	token = strtok_r(ptr, ".", &rest);
	job1 = atoi(token);
	
	fsd_free(token);
	
	ptr = fsd_strdup(s2);
	token = strtok_r(ptr,".",&rest);
	job2 = atoi(token);
	
	fsd_free(token);
	return job1 - job2;
}

