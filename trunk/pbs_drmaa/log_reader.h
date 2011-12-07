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
 
#ifndef __PBS_DRMAA__LOG_READER_H
#define __PBS_DRMAA__LOG_READER_H

#ifdef HAVE_CONFIG_H
#	include <config.h>
#endif

#include <stdio.h>

#include <drmaa_utils/job.h>
#include <drmaa_utils/session.h>

typedef struct pbsdrmaa_log_reader_s pbsdrmaa_log_reader_t;

pbsdrmaa_log_reader_t * 
pbsdrmaa_log_reader_new ( fsd_drmaa_session_t * session);

void
pbsdrmaa_log_reader_destroy ( pbsdrmaa_log_reader_t * self );

struct pbsdrmaa_log_reader_s {
	fsd_drmaa_session_t *volatile session ;
	
	void (*read_log) ( pbsdrmaa_log_reader_t * self );
	
	void (*select_file) ( pbsdrmaa_log_reader_t * self );
	
	void (*close) ( pbsdrmaa_log_reader_t * self );

	void (*reopen) ( pbsdrmaa_log_reader_t * self );


	/* determines if function should run */
	bool run_flag;
	
	/* date of current file */
	time_t t;	
	
	/* log file handle */
	FILE *fhandle;
	
	/* for wait_thread - day changed */
	bool volatile date_changed;
	
	/* for wait_thread - log file first open */
	bool volatile first_open;	

	char *volatile log_path;

	off_t volatile current_offset;
};

#endif /* __PBS_DRMAA__LOG_READER_H */
