/* $Id$ */
/*
 *  PSNC DRMAA 2.0 for Torque/PBS Pro
 *  Copyright (C) 2012  Poznan Supercomputing and Networking Center
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

#include <drmaa_utils/drmaa_base.h>



#if 0
static fsd_drmaa_session_t *
pbsdrmaa_new_session( fsd_drmaa_singletone_t *self, const char *contact )
{
	return pbsdrmaa_session_new( contact );
}

static fsd_template_t *
pbsdrmaa_new_job_template( fsd_drmaa_singletone_t *self )
{
	return drmaa_template_new();
}

static const char *
pbsdrmaa_get_contact( fsd_drmaa_singletone_t *self )
{
	const char *contact = NULL;
	fsd_mutex_lock( &self->session_mutex );
	if( self->session )
		contact = self->session->contact;
	if( contact == NULL )
		contact = "localhost";
	fsd_mutex_unlock( &self->session_mutex );
	return contact;
}

static void
pbsdrmaa_get_version( fsd_drmaa_singletone_t *self,
		unsigned *major, unsigned *minor )
{
	*major = 1;  *minor = 0;
}

static const char *
pbsdrmaa_get_DRM_system( fsd_drmaa_singletone_t *self )
{
#ifdef PBS_PROFESSIONAL
	return "PBS Professional";
#else
	return "Torque";
#endif
}

static const char *
pbsdrmaa_get_DRMAA_implementation( fsd_drmaa_singletone_t *self )
{
	return PACKAGE_NAME" v. "PACKAGE_VERSION
					" <http://sourceforge.net/projects/pbspro-drmaa/>";
}


fsd_iter_t *
pbsdrmaa_get_attribute_names( fsd_drmaa_singletone_t *self )
{
	static const char *attribute_names[] = {
		DRMAA_REMOTE_COMMAND,
		DRMAA_JS_STATE,
		DRMAA_WD,
		DRMAA_JOB_CATEGORY,
		DRMAA_NATIVE_SPECIFICATION,
		DRMAA_BLOCK_EMAIL,
		DRMAA_START_TIME,
		DRMAA_JOB_NAME,
		DRMAA_INPUT_PATH,
		DRMAA_OUTPUT_PATH,
		DRMAA_ERROR_PATH,
		DRMAA_JOIN_FILES,
		DRMAA_TRANSFER_FILES,
		DRMAA_WCT_HLIMIT,
		DRMAA_DURATION_HLIMIT,
		NULL
	};
	return fsd_iter_new_const( attribute_names, -1 );
}

fsd_iter_t *
pbsdrmaa_get_vector_attribute_names( fsd_drmaa_singletone_t *self )
{
	static const char *attribute_names[] = {
		DRMAA_V_ARGV,
		DRMAA_V_ENV,
		DRMAA_V_EMAIL,
		NULL
	};
	return fsd_iter_new_const( attribute_names, -1 );
}

static int
pbsdrmaa_wifexited(
		int *exited, int stat,
		char *error_diagnosis, size_t error_diag_len
		)
{
	*exited = (stat <= 125);
	return DRMAA_ERRNO_SUCCESS;
}

static int
pbsdrmaa_wexitstatus(
		int *exit_status, int stat,
		char *error_diagnosis, size_t error_diag_len
		)
{
	*exit_status = stat & 0xff;
	return DRMAA_ERRNO_SUCCESS;
}

static int
pbsdrmaa_wifsignaled(
		int *signaled, int stat,
		char *error_diagnosis, size_t error_diag_len
		)
{
	*signaled = (stat > 128 );
	return DRMAA_ERRNO_SUCCESS;
}	

static int
pbsdrmaa_wtermsig(
		char *signal, size_t signal_len, int stat,
		char *error_diagnosis, size_t error_diag_len
		)
{
	int sig = stat & 0x7f;
	strlcpy( signal, fsd_strsignal(sig), signal_len );
	return DRMAA_ERRNO_SUCCESS;
}

static int
pbsdrmaa_wcoredump(
		int *core_dumped, int stat,
		char *error_diagnosis, size_t error_diag_len
		)
{
	*core_dumped = 0;
	return DRMAA_ERRNO_SUCCESS;
}

static int
pbsdrmaa_wifaborted(
		int *aborted, int stat,
		char *error_diagnosis, size_t error_diag_len
		)
{
	fsd_log_debug(("wifaborted(%d)", stat));

	if ( stat == -1 )
	 {
		*aborted = true;
	 }
	else if ( stat <= 125 )
	 {
		*aborted = false;
	 }
	else if ( stat == 126 || stat == 127 )
         {
		*aborted = true;
	 } 
	else switch( stat & 0x7f )
	 {
		case SIGTERM:  case SIGKILL:
			*aborted = true;
			break;
		default:
			*aborted = false;
			break;
	 }
	return DRMAA_ERRNO_SUCCESS;
}


fsd_drmaa_singletone_t _fsd_drmaa_singletone = {
	NULL,
	FSD_MUTEX_INITIALIZER,

	pbsdrmaa_new_session,
	pbsdrmaa_new_job_template,

	pbsdrmaa_get_contact,
	pbsdrmaa_get_version,
	pbsdrmaa_get_DRM_system,
	pbsdrmaa_get_DRMAA_implementation,

	pbsdrmaa_get_attribute_names,
	pbsdrmaa_get_vector_attribute_names,

	pbsdrmaa_wifexited,
	pbsdrmaa_wexitstatus,
	pbsdrmaa_wifsignaled,
	pbsdrmaa_wtermsig,
	pbsdrmaa_wcoredump,
	pbsdrmaa_wifaborted
};

#endif
