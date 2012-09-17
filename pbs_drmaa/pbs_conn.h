/* $Id$ */
/*
 *  PSNC DRMAA for Torque/PBS Pro
 *  Copyright (C) 2012 Poznan Supercomputing and Networking Center
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
 
#ifndef __PBS_DRMAA__PBS_CONN_H
#define __PBS_DRMAA__PBS_CONN_H

#ifdef HAVE_CONFIG_H
#	include <config.h>
#endif

#include <stdio.h>

#include <drmaa_utils/job.h>
#include <drmaa_utils/session.h>

#include <session.h>

#include <pbs_ifl.h>

typedef struct pbsdrmaa_pbs_conn_s pbsdrmaa_pbs_conn_t;

pbsdrmaa_pbs_conn_t * pbsdrmaa_pbs_conn_new ( pbsdrmaa_session_t * session, char *server);

void
pbsdrmaa_pbs_conn_destroy ( pbsdrmaa_pbs_conn_t * self );

struct pbsdrmaa_pbs_conn_s {
	pbsdrmaa_session_t *volatile session;

	char* (*submit) ( pbsdrmaa_pbs_conn_t *self, struct attropl *attrib, char *script, char *destination );

	struct batch_status* (*statjob) ( pbsdrmaa_pbs_conn_t *self,  char *job_id, struct attrl *attrib );

	void (*statjob_free) ( pbsdrmaa_pbs_conn_t *self, struct batch_status* job_status );

	void (*sigjob) ( pbsdrmaa_pbs_conn_t *self, char *job_id, char *signal );

	void (*deljob) ( pbsdrmaa_pbs_conn_t *self, char *job_id );

	void (*rlsjob) ( pbsdrmaa_pbs_conn_t *self, char *job_id );

	void (*holdjob) ( pbsdrmaa_pbs_conn_t *self, char *job_id );
	
	/* contact string */
	char *server;
	/* connection descriptor */
	int connection_fd;
	
	/* timestamp of last usage */
	time_t last_usage;	
};

#endif /* __PBS_DRMAA__PBS_CONN_H */
