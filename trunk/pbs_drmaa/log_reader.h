/* $Id: log_reader.h 323 2010-09-21 21:31:29Z mmatloka $ */
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
 
#ifndef __PBS_DRMAA__LOG_READER_H
#define __PBS_DRMAA__LOG_READER_H

#ifdef HAVE_CONFIG_H
#	include <config.h>
#endif

#include <drmaa_utils/job.h>
#include <drmaa_utils/session.h>

typedef struct pbsdrmaa_log_reader_s pbsdrmaa_log_reader_t;

pbsdrmaa_log_reader_t * 
pbsdrmaa_log_reader_new ( fsd_drmaa_session_t * session, fsd_job_t * job );

void
pbsdrmaa_log_reader_destroy ( pbsdrmaa_log_reader_t * self );

struct pbsdrmaa_log_reader_s {
	fsd_drmaa_session_t *volatile session ;
	fsd_job_t *volatile job;
	
	void (*
	read_log) ( pbsdrmaa_log_reader_t * self );
	
	void (*
	chose_file) ( pbsdrmaa_log_reader_t * self );
	
	ssize_t (*
	read_line) ( pbsdrmaa_log_reader_t * self , char * buffer , ssize_t size );
	
	/* specifies if function should run */
	bool run_flag;
	
	/* date of current file */
	time_t t;	
	
	/* for job_on_missing  - available log files */
	char ** log_files;
	
	/* for job_on_missing - number of log files */
	int log_files_number;
	
	/* log file descriptor */
	int volatile fd;
	
	/* for job_on_missing - log file size when function was ran */
	off_t log_file_initial_size;
	
	/* for job_on_missing - read lines size */
	off_t log_file_read_size;
	
	/* for wait_thread - day changed */
	bool volatile date_changed;
	
	/* for wait_thread - log file first open */
	bool volatile first_open;	
	
	char * name;
};

#endif /* __PBS_DRMAA__LOG_READER_H */
