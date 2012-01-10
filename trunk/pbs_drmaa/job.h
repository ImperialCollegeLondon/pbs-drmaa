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

#ifndef __PBS_DRMAA__JOB_H
#define __PBS_DRMAA__JOB_H

#ifdef HAVE_CONFIG_H
#	include <config.h>
#endif

#include <drmaa_utils/job.h>

typedef struct pbsdrmaa_job_s pbsdrmaa_job_t;

/* Do not depend on <pbs_ifl.h> */
struct batch_status;

fsd_job_t *
pbsdrmaa_job_new( char *job_id );

struct pbsdrmaa_job_s {
	fsd_job_t super;
	time_t missing_time; /* The time when job was seen as missing for the first time */

	void (*
	update)( fsd_job_t *self, struct batch_status *status );
};


#endif /* __PBS_DRMAA__JOB_H */

