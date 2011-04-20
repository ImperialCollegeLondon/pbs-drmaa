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

#ifndef __DRMAA__PBS_DRMAA_H
#define __DRMAA__PBS_DRMAA_H

#ifdef HAVE_CONFIG_H
#	include <config.h>
#endif

#include <sys/types.h>
#include <stdbool.h>
#include <time.h>
#include <pbs_ifl.h>

#include <drmaa_utils/common.h>
#include <pbs_drmaa/pbs_attrib.h>


/** PBS DRMAA specific session data. */
struct drmaa_session_impl_s {
	int                 pbs_conn;    /**< PBS connection (or -1). */
	drmaa_mutex_t       conn_mutex;  /**< Mutex for PBS connection. */
	drmaa_thread_t      wait_thread;
	bool                wait_thread_started;
	struct attrl       *status_attrl;
};


enum { PBS_ATTRIBS_BITSET_SIZE = (N_PBS_DRMAA_ATTRIBS+31) / 32 };
struct drmaa_submit_impl_s {
	struct attrl *pbs_attribs;
	uint32_t pbs_attribs_bitset[ PBS_ATTRIBS_BITSET_SIZE ];
	char *script_filename;
};


void *
drmaa_wait_thread( void *arg );

drmaa_submit_ctx_t *
drmaa_create_submission_context(
		const drmaa_job_template_t *jt, int bulk_no,
		drmaa_err_ctx_t *err
		);

void
drmaa_free_submission_context( drmaa_submit_ctx_t *c, drmaa_err_ctx_t* );

void drmaa_set_job_defaults        ( drmaa_submit_ctx_t*, drmaa_err_ctx_t* );
void drmaa_create_job_script       ( drmaa_submit_ctx_t*, drmaa_err_ctx_t* );
void drmaa_set_job_submit_state    ( drmaa_submit_ctx_t*, drmaa_err_ctx_t* );
void drmaa_set_job_files           ( drmaa_submit_ctx_t*, drmaa_err_ctx_t* );
void drmaa_set_file_staging        ( drmaa_submit_ctx_t*, drmaa_err_ctx_t* );
void drmaa_set_job_resources       ( drmaa_submit_ctx_t*, drmaa_err_ctx_t* );
void drmaa_set_job_environment     ( drmaa_submit_ctx_t*, drmaa_err_ctx_t* );
void drmaa_set_job_email_notif     ( drmaa_submit_ctx_t*, drmaa_err_ctx_t* );
void drmaa_apply_native_spec       ( drmaa_submit_ctx_t*, drmaa_err_ctx_t* );
void drmaa_apply_job_category      ( drmaa_submit_ctx_t*, drmaa_err_ctx_t* );
char *drmaa_translate_staging( const char *stage, drmaa_err_ctx_t* );

void
drmaa_parse_qsub_args(
		drmaa_submit_ctx_t *ctx, const char *string,
		drmaa_err_ctx_t *err
		);

void
drmaa_job_missing( drmaa_job_t *job, drmaa_err_ctx_t *err );

void
drmaa_add_pbs_attr(
		drmaa_submit_ctx_t *c,
		int attr, char *resource, char *value,
		unsigned set,
		drmaa_err_ctx_t *err
		);

char *
drmaa_write_tmpfile(
	 const char *content, size_t len,
	 drmaa_err_ctx_t *err
	 );

struct attrl *
drmaa_create_pbs_attrl( drmaa_err_ctx_t *err );

void
drmaa_log_attrl( const struct attrl *attribute_list, const char *prefix );

void
drmaa_free_attrl( struct attrl *attr );

void
drmaa_err_pbs_error( drmaa_err_ctx_t *err );

int
drmaa_err_map_pbs_error( int pbs_errcode );

#define RAISE_PBS() \
	drmaa_err_pbs_error( err )

#endif /* __DRMAA__PBS_DRMAA_H */

