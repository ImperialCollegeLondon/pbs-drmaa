/* $Id: submit.h 338 2010-09-28 14:48:45Z mamonski $ */
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

#ifndef __PBS_DRMAA__SUBMIT_H
#define __PBS_DRMAA__SUBMIT_H

#ifdef HAVE_CONFIG_H
#	include <config.h>
#endif

#include <drmaa_utils/drmaa_util.h>

typedef struct pbsdrmaa_submit_s pbsdrmaa_submit_t;

pbsdrmaa_submit_t *
pbsdrmaa_submit_new( fsd_drmaa_session_t *session,
		const fsd_template_t *job_template, int bulk_idx );

struct pbsdrmaa_submit_s {
	void (*
	destroy)( pbsdrmaa_submit_t *self );

	char * (*
	submit)( pbsdrmaa_submit_t *self );

	void (*
	eval)( pbsdrmaa_submit_t *self );

	void (*apply_defaults)( pbsdrmaa_submit_t *self );
	void (*apply_job_script)( pbsdrmaa_submit_t *self );
	void (*apply_job_state)( pbsdrmaa_submit_t *self );
	void (*apply_job_files)( pbsdrmaa_submit_t *self );
	void (*apply_file_staging)( pbsdrmaa_submit_t *self );
	void (*apply_job_resources)( pbsdrmaa_submit_t *self );
	void (*apply_job_environment)( pbsdrmaa_submit_t *self );
	void (*apply_email_notification)( pbsdrmaa_submit_t *self );
	void (*apply_job_category)( pbsdrmaa_submit_t *self );
	void (*apply_native_specification)(
			pbsdrmaa_submit_t *self, const char *native_specification );

	void (*set)( pbsdrmaa_submit_t *self, const char *pbs_attr,
			char *value, unsigned placeholders );

	fsd_drmaa_session_t *session;
	const fsd_template_t *job_template;
	char *script_filename;
	char *destination_queue;
	fsd_template_t *pbs_job_attributes;
	fsd_expand_drmaa_ph_t *expand_ph;
	/* struct attrl *pbs_attribs; */
	/* uint32_t pbs_attribs_bitset[ PBS_ATTRIBS_BITSET_SIZE ]; */
};

void pbsdrmaa_submit_apply_native_specification(
		pbsdrmaa_submit_t *self, const char *native_specification );

#endif /* __PBS_DRMAA__SUBMIT_H */

