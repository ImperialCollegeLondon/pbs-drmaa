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

#ifdef HAVE_CONFIG_H
#	include <config.h>
#endif

#include <unistd.h>
#include <string.h>

#include <pbs_ifl.h>
#include <pbs_error.h>

#include <drmaa_utils/conf.h>
#include <drmaa_utils/drmaa.h>
#include <drmaa_utils/drmaa_util.h>
#include <drmaa_utils/datetime.h>
#include <drmaa_utils/iter.h>
#include <drmaa_utils/template.h>
#include <pbs_drmaa/pbs_attrib.h>
#include <pbs_drmaa/session.h>
#include <pbs_drmaa/submit.h>
#include <pbs_drmaa/util.h>



#ifndef lint
static char rcsid[]
#	ifdef __GNUC__
		__attribute__ ((unused))
#	endif
	= "$Id$";
#endif

static void
pbsdrmaa_submit_destroy( pbsdrmaa_submit_t *self );

static char *
pbsdrmaa_submit_submit( pbsdrmaa_submit_t *self );

static void
pbsdrmaa_submit_eval( pbsdrmaa_submit_t *self );


static void
pbsdrmaa_submit_set( pbsdrmaa_submit_t *self, const char *pbs_attr,
		char *value, unsigned placeholders );

static void pbsdrmaa_submit_apply_defaults( pbsdrmaa_submit_t *self );
static void pbsdrmaa_submit_apply_job_script( pbsdrmaa_submit_t *self );
static void pbsdrmaa_submit_apply_job_state( pbsdrmaa_submit_t *self );
static void pbsdrmaa_submit_apply_job_files( pbsdrmaa_submit_t *self );
static void pbsdrmaa_submit_apply_file_staging( pbsdrmaa_submit_t *self );
static void pbsdrmaa_submit_apply_job_resources( pbsdrmaa_submit_t *self );
static void pbsdrmaa_submit_apply_job_environment( pbsdrmaa_submit_t *self );
static void pbsdrmaa_submit_apply_email_notification( pbsdrmaa_submit_t *self );
static void pbsdrmaa_submit_apply_job_category( pbsdrmaa_submit_t *self );


pbsdrmaa_submit_t *
pbsdrmaa_submit_new( fsd_drmaa_session_t *session,
		const fsd_template_t *job_template, int bulk_idx )
{
	pbsdrmaa_submit_t *volatile self = NULL;
	TRY
	 {
		fsd_malloc( self, pbsdrmaa_submit_t );
		self->session = session;
		self->job_template = job_template;
		self->script_filename = NULL;
		self->destination_queue = NULL;
		self->pbs_job_attributes = NULL;
		self->expand_ph = NULL;
		self->destroy = pbsdrmaa_submit_destroy;
		self->submit = pbsdrmaa_submit_submit;
		self->eval = pbsdrmaa_submit_eval;
		self->set = pbsdrmaa_submit_set;
		self->apply_defaults = pbsdrmaa_submit_apply_defaults;
		self->apply_job_category = pbsdrmaa_submit_apply_job_category;
		self->apply_job_script = pbsdrmaa_submit_apply_job_script;
		self->apply_job_state = pbsdrmaa_submit_apply_job_state;
		self->apply_job_files = pbsdrmaa_submit_apply_job_files;
		self->apply_file_staging = pbsdrmaa_submit_apply_file_staging;
		self->apply_job_resources = pbsdrmaa_submit_apply_job_resources;
		self->apply_job_environment = pbsdrmaa_submit_apply_job_environment;
		self->apply_email_notification = pbsdrmaa_submit_apply_email_notification;
		self->apply_native_specification =
			pbsdrmaa_submit_apply_native_specification;

		self->pbs_job_attributes = pbsdrmaa_pbs_template_new();
		self->expand_ph = fsd_expand_drmaa_ph_new( NULL, NULL,
				(bulk_idx >= 0) ? fsd_asprintf("%d", bulk_idx) : NULL );
	 }
	EXCEPT_DEFAULT
	 {
		if( self )
			self->destroy( self );
	 }
	END_TRY
	return self;
}


void
pbsdrmaa_submit_destroy( pbsdrmaa_submit_t *self )
{
	if( self->script_filename )
	 {
		unlink( self->script_filename );
		fsd_free( self->script_filename );
	 }
	if( self->pbs_job_attributes )
		self->pbs_job_attributes->destroy( self->pbs_job_attributes );
	if( self->expand_ph )
		self->expand_ph->destroy( self->expand_ph );
	fsd_free( self->destination_queue );
	fsd_free( self );
}


char *
pbsdrmaa_submit_submit( pbsdrmaa_submit_t *self )
{
	volatile bool conn_lock = false;
	struct attrl *volatile pbs_attr = NULL;
	char *volatile job_id = NULL;
	TRY
	 {
		const fsd_template_t *pbs_tmpl = self->pbs_job_attributes;
		unsigned i;

		for( i = 0;  i < PBSDRMAA_N_PBS_ATTRIBUTES;  i++ )
		 {
			const char *name = pbs_tmpl->by_code( pbs_tmpl, i )->name;
			if( name  &&  name[0] != '!' && pbs_tmpl->get_attr( pbs_tmpl, name ) )
			 {
				const char *value;

				value = pbs_tmpl->get_attr( pbs_tmpl, name );
				pbs_attr = pbsdrmaa_add_attr( pbs_attr, name, value );
			 }
		 }

		conn_lock = fsd_mutex_lock( &self->session->drm_connection_mutex );
retry:
		job_id = pbs_submit( ((pbsdrmaa_session_t*)self->session)->pbs_conn,
				(struct attropl*)pbs_attr, self->script_filename,
				self->destination_queue, NULL );

		fsd_log_info(("pbs_submit(%s, %s) =%s", self->script_filename, self->destination_queue, job_id));

		if( job_id == NULL )
		{
			if (pbs_errno == PBSE_PROTOCOL || pbs_errno == PBSE_EXPIRED)
			 {
				pbsdrmaa_session_t *pbsself = (pbsdrmaa_session_t*)self->session;
				if (pbsself->pbs_conn >= 0 )
					pbs_disconnect( pbsself->pbs_conn );
				sleep(1);
				pbsself->pbs_conn = pbs_connect( pbsself->super.contact );
				if( pbsself->pbs_conn < 0 )
					pbsdrmaa_exc_raise_pbs( "pbs_connect" );
				else
					goto retry;
			 }
			else
			 {
				pbsdrmaa_exc_raise_pbs( "pbs_submit" );
			 }
		}
		conn_lock = fsd_mutex_unlock( &self->session->drm_connection_mutex );
	 }
	EXCEPT_DEFAULT
	 {
		fsd_free( job_id );
		fsd_exc_reraise();
	 }
	FINALLY
	 {
		if( conn_lock )
			conn_lock = fsd_mutex_unlock( &self->session->drm_connection_mutex );
		if( pbs_attr )
			pbsdrmaa_free_attrl( pbs_attr );
	 }
	END_TRY
	return job_id;
}


void
pbsdrmaa_submit_eval( pbsdrmaa_submit_t *self )
{
	self->apply_defaults( self );
	self->apply_job_category( self );
	self->apply_job_script( self );
	self->apply_job_state( self );
	self->apply_job_files( self );
	self->apply_file_staging( self );
	self->apply_job_resources( self );
	self->apply_job_environment( self );
	self->apply_email_notification( self );
	self->apply_native_specification( self, NULL );
}


void
pbsdrmaa_submit_set( pbsdrmaa_submit_t *self, const char *name,
		 char *value, unsigned placeholders )
{
	fsd_template_t *pbs_attr = self->pbs_job_attributes;
	TRY
	 {
		if( placeholders )
			value = self->expand_ph->expand(
					self->expand_ph, value, placeholders );
		pbs_attr->set_attr( pbs_attr, name, value );
	 }
	FINALLY
	 {
		fsd_free( value );
	 }
	END_TRY
}


void
pbsdrmaa_submit_apply_defaults( pbsdrmaa_submit_t *self )
{
	fsd_template_t *pbs_attr = self->pbs_job_attributes;
	pbs_attr->set_attr( pbs_attr, PBSDRMAA_CHECKPOINT, "u" );
	pbs_attr->set_attr( pbs_attr, PBSDRMAA_KEEP_FILES, "n" );
	pbs_attr->set_attr( pbs_attr, PBSDRMAA_PRIORITY, "0" );
}


void
pbsdrmaa_submit_apply_job_script( pbsdrmaa_submit_t *self )
{
	const fsd_template_t *jt = self->job_template;
	/* fsd_template_t *pbs_attr = self->pbs_job_attributes; */
	fsd_expand_drmaa_ph_t *expand = self->expand_ph;
	char *script = NULL;
	size_t script_len;
	const char *executable;
	const char *wd;
	const char *const *argv;
	const char *input_path;
	const char *const *i;

	executable   = jt->get_attr( jt, DRMAA_REMOTE_COMMAND );
	wd           = jt->get_attr( jt, DRMAA_WD );
	argv         = jt->get_v_attr( jt, DRMAA_V_ARGV );
	input_path   = jt->get_attr( jt, DRMAA_INPUT_PATH );

	if( wd )
	 {
		char *cwd = NULL;
		cwd = expand->expand( expand, fsd_strdup(wd),
				FSD_DRMAA_PH_HD | FSD_DRMAA_PH_INCR );
		expand->set( expand, FSD_DRMAA_PH_WD, cwd );
	 }

	if( executable == NULL )
		fsd_exc_raise_code( FSD_DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE );

	if( input_path != NULL )
	 {
		if( input_path[0] == ':' )
			input_path++;
	 }

	 { /* compute script length */
		script_len = 0;
		if( wd != NULL )
			script_len += strlen("cd ") + strlen(wd) + strlen("; ");
		script_len += strlen("exec ") + strlen(executable);
		if( argv != NULL )
			for( i = argv;  *i != NULL;  i++ )
				script_len += 3+strlen(*i);
		if( input_path != NULL )
			script_len += strlen(" <") + strlen(input_path);
	 }

	fsd_calloc( script, script_len+1, char );

	 {
		char *s;
		s = script;
		if( wd != NULL )
			s += sprintf( s, "cd %s; ", wd );
		s += sprintf( s, "exec %s", executable );
		if( argv != NULL )
			for( i = argv;  *i != NULL;  i++ )
				s += sprintf( s, " '%s'", *i );
		if( input_path != NULL )
			s += sprintf( s, " <%s", input_path );
		fsd_assert( s == script+script_len );
	 }

	script = expand->expand( expand, script,
			FSD_DRMAA_PH_HD | FSD_DRMAA_PH_WD | FSD_DRMAA_PH_INCR );

	/* pbs_attr->set_attr( pbs_attr, "!script", script ); */

	self->script_filename = pbsdrmaa_write_tmpfile( script, strlen(script) );
	fsd_free( script );
}


void
pbsdrmaa_submit_apply_job_state( pbsdrmaa_submit_t *self )
{
	const fsd_template_t *jt = self->job_template;
	fsd_template_t *pbs_attr = self->pbs_job_attributes;
	const char *job_name = NULL;
	const char *submit_state = NULL;
	const char *drmaa_start_time = NULL;

	job_name = jt->get_attr( jt, DRMAA_JOB_NAME );
	submit_state = jt->get_attr( jt, DRMAA_JS_STATE );
	drmaa_start_time = jt->get_attr( jt, DRMAA_START_TIME );

	if( job_name != NULL )
		pbs_attr->set_attr( pbs_attr, PBSDRMAA_JOB_NAME, job_name );

	if( submit_state != NULL )
	 {
		const char *hold_types;
		if( !strcmp(submit_state, DRMAA_SUBMISSION_STATE_ACTIVE) )
			hold_types = "n";
		else if( !strcmp(submit_state, DRMAA_SUBMISSION_STATE_HOLD) )
			hold_types = "u";
		else
			fsd_exc_raise_fmt( FSD_ERRNO_INVALID_VALUE,
					"invalid value of %s attribute (%s|%s)",
					DRMAA_JS_STATE, DRMAA_SUBMISSION_STATE_ACTIVE,
					DRMAA_SUBMISSION_STATE_HOLD );
		pbs_attr->set_attr( pbs_attr, PBSDRMAA_HOLD_TYPES, hold_types );
	 }

	if( drmaa_start_time != NULL )
	 {
		time_t start_time;
		char pbs_start_time[20];
		struct tm start_time_tm;
		start_time = fsd_datetime_parse( drmaa_start_time );
		localtime_r( &start_time, &start_time_tm );
		sprintf( pbs_start_time, "%04d%02d%02d%02d%02d.%02d",
				start_time_tm.tm_year + 1900,
				start_time_tm.tm_mon + 1,
				start_time_tm.tm_mday,
				start_time_tm.tm_hour,
				start_time_tm.tm_min,
				start_time_tm.tm_sec
				);
		pbs_attr->set_attr( pbs_attr, PBSDRMAA_EXECUTION_TIME, pbs_start_time );
	 }
}


void
pbsdrmaa_submit_apply_job_files( pbsdrmaa_submit_t *self )
{
	const fsd_template_t *jt = self->job_template;
	fsd_template_t *pbs_attr = self->pbs_job_attributes;
	const char *join_files;
	bool b_join_files;
	int i;

	for( i = 0;  i < 2;  i++ )
	 {
		const char *drmaa_name;
		const char *pbs_name;
		const char *path;

		if( i == 0 )
		 {
			drmaa_name = DRMAA_OUTPUT_PATH;
			pbs_name = PBSDRMAA_OUTPUT_PATH;
		 }
		else
		 {
			drmaa_name = DRMAA_ERROR_PATH;
			pbs_name = PBSDRMAA_ERROR_PATH;
		 }

		path = jt->get_attr( jt, drmaa_name );
		if( path != NULL )
		 {
			if( path[0] == ':' )
				path++;
			self->set(self, pbs_name, fsd_strdup(path), FSD_DRMAA_PH_HD | FSD_DRMAA_PH_WD | FSD_DRMAA_PH_INCR);
		 }
	 }

	join_files = jt->get_attr( jt, DRMAA_JOIN_FILES );
	b_join_files = join_files != NULL  &&  !strcmp(join_files,"1");
	pbs_attr->set_attr( pbs_attr, PBSDRMAA_JOIN_FILES, (b_join_files ? "y" : "n") ); 
}


void
pbsdrmaa_submit_apply_file_staging( pbsdrmaa_submit_t *self )
{
	/* TODO */
}


void
pbsdrmaa_submit_apply_job_resources( pbsdrmaa_submit_t *self )
{
	const fsd_template_t *jt = self->job_template;
	fsd_template_t *pbs_attr = self->pbs_job_attributes;
	const char *cpu_time_limit = NULL;
	const char *walltime_limit = NULL;

	cpu_time_limit = jt->get_attr( jt, DRMAA_DURATION_HLIMIT );
	walltime_limit = jt->get_attr( jt, DRMAA_WCT_HLIMIT );
	if( cpu_time_limit )
	 {
		pbs_attr->set_attr( pbs_attr, "Resource_List.pcput", cpu_time_limit );
		pbs_attr->set_attr( pbs_attr, "Resource_List.cput", cpu_time_limit );
	 }
	if( walltime_limit )
		pbs_attr->set_attr( pbs_attr, "Resource_List.walltime", walltime_limit );
}

void
pbsdrmaa_submit_apply_job_environment( pbsdrmaa_submit_t *self )
{
	const fsd_template_t *jt = self->job_template;
	const char *const *env_v;
	const char *jt_wd;
	char *wd;
	char *env_c = NULL;
	int ii = 0, len = 0;

	env_v = jt->get_v_attr( jt, DRMAA_V_ENV);
	jt_wd    = jt->get_attr( jt, DRMAA_WD );
	
	if (!jt_wd)
	{
		wd = fsd_getcwd();
	}
	else
	{
		wd = fsd_strdup(jt_wd);
	}

	if (env_v)
	{
		ii = 0;
		while (env_v[ii]) {
			len += strlen(env_v[ii]) + 1;
			ii++;
		}
	}
	
	len+= strlen("PBS_O_WORKDIR=") + strlen(wd);

	fsd_calloc(env_c, len + 1, char);
	env_c[0] = '\0';

	if (env_v)
	{
		ii = 0;
		while (env_v[ii]) {
			strcat(env_c, env_v[ii]);
			strcat(env_c, ",");
			ii++;
		}

	}
	
	strcat(env_c, "PBS_O_WORKDIR=");
	strcat(env_c, wd);

	self->pbs_job_attributes->set_attr(self->pbs_job_attributes, "Variable_List", env_c);

	fsd_free(env_c);
	fsd_free(wd);
}


void
pbsdrmaa_submit_apply_email_notification( pbsdrmaa_submit_t *self )
{
	/* TODO */
}


void
pbsdrmaa_submit_apply_job_category( pbsdrmaa_submit_t *self )
{
	const char *job_category = NULL;
	const char *category_spec = NULL;
	fsd_conf_option_t *value = NULL;

	job_category = self->job_template->get_attr(
			self->job_template, DRMAA_JOB_CATEGORY );
	if( job_category == NULL  ||  job_category[0] == '\0' )
		job_category = "default";
	value = fsd_conf_dict_get( self->session->job_categories,
			job_category );
	if( value != NULL  &&  value->type == FSD_CONF_STRING )
		category_spec = value->val.string;
	if( category_spec != NULL )
		self->apply_native_specification( self, category_spec );
}

static void parse_resources(fsd_template_t *pbs_attr,const char *resources)
{
	char * volatile name = NULL;
	char *arg = NULL;
	char *value = NULL;
	char *ctxt = NULL;
	char * volatile resources_copy = fsd_strdup(resources);

	TRY
	  {
		for (arg = strtok_r(resources_copy, ",", &ctxt); arg; arg = strtok_r(NULL, ",",&ctxt) )
		{
			char *psep = strchr(arg, '=');

			if (psep) 
			{
				*psep = '\0';
				name = fsd_asprintf("Resource_List.%s", arg);
				value = ++psep;
				pbs_attr->set_attr( pbs_attr, name , value );
				fsd_free(name);
				name = NULL;
			}
			else
			{
				fsd_exc_raise_fmt(FSD_DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE, "Invalid native specification: %s (Invalid resource specification: %s)", resources, arg);
			}
		}
	  }
	FINALLY
	  {
		fsd_free(name);
		fsd_free(resources_copy);
	  }
	END_TRY
}

static void parse_additional_attr(fsd_template_t *pbs_attr,const char *add_attr)
{
	char * volatile name = NULL;
	char *arg = NULL;
	char *value = NULL;
	char *ctxt = NULL, *ctxt2 = NULL;
	char * volatile add_attr_copy = fsd_strdup(add_attr);

	TRY
	  {
		for (arg = strtok_r(add_attr_copy, ";", &ctxt); arg; arg = strtok_r(NULL, ";",&ctxt) )
		{
			name = fsd_strdup(strtok_r(arg, "=", &ctxt2));
			value = strtok_r(NULL, "=", &ctxt2);
			pbs_attr->set_attr( pbs_attr, name , value );
			fsd_free(name);
			name = NULL;
		}
	  }
	FINALLY
	  {
		fsd_free(name);
		fsd_free(add_attr_copy);
	  }
	END_TRY
}


void
pbsdrmaa_submit_apply_native_specification( pbsdrmaa_submit_t *self,
		const char *native_specification )
{
        fsd_log_enter(( "({native_specification=%s})", native_specification ));

	if( native_specification == NULL )
		native_specification = self->job_template->get_attr(
				self->job_template, DRMAA_NATIVE_SPECIFICATION );
	if( native_specification == NULL )
		return;

	{
		fsd_iter_t * volatile args_list = fsd_iter_new(NULL, 0);
		fsd_template_t *pbs_attr = self->pbs_job_attributes;
		char *arg = NULL;
		volatile char * native_spec_copy = fsd_strdup(native_specification);
		char * ctxt = NULL;
		int opt = 0;

		TRY
		  {
			for (arg = strtok_r(native_spec_copy, " \t", &ctxt); arg; arg = strtok_r(NULL, " \t",&ctxt) ) {
				if (!opt)
				  {
					if ( (arg[0] != '-') || (strlen(arg) != 2) )
						fsd_exc_raise_fmt(FSD_DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE,
							"Invalid native specification: -o(ption) expected (arg=%s native=%s).",
							arg, native_specification);

					opt = arg[1];

					/* handle NO-arg options */

					switch (opt) {
						case 'h' :
							pbs_attr->set_attr( pbs_attr, "Hold_Types" , "u" );
							break;
						default :
							continue; /*no NO-ARG option */
					}

					opt = 0;
				  }
				else
				  {
					switch (opt) {
						
						case 'W' :
							parse_additional_attr(pbs_attr, arg);
							break;
						case 'N' :
							pbs_attr->set_attr( pbs_attr, "Job_Name" , arg );
							break;
						case 'o' :
							pbs_attr->set_attr( pbs_attr, "Output_Path" , arg );
							break;
						case 'e' :
							pbs_attr->set_attr( pbs_attr, "Error_Path" , arg );
							break;
						case 'j' :
							pbs_attr->set_attr( pbs_attr, "Join_Path" , arg );
							break;
						case 'm' :
							pbs_attr->set_attr( pbs_attr, "Mail_Points" , arg );
							break;
						case 'a' :
							pbs_attr->set_attr( pbs_attr, "Execution_Time" , arg );
							break;
						case 'A' :
							pbs_attr->set_attr( pbs_attr, "Account_Name" , arg );
							break;
						case 'c' :
							pbs_attr->set_attr( pbs_attr, "Checkpoint" , arg );
							break;
						case 'k' :
							pbs_attr->set_attr( pbs_attr, "Keep_Files" , arg );
							break;
						case 'p' :
							pbs_attr->set_attr( pbs_attr, "Priority" , arg );
							break;
						case 'q' :
							if (self->destination_queue)
								fsd_free(self->destination_queue);

							self->destination_queue = fsd_strdup( arg );
							fsd_log_debug(("self->destination_queue = %s", self->destination_queue));
							break;
						case 'r' :
							pbs_attr->set_attr( pbs_attr, "Rerunable" , arg );
							break;
						case 'S' :
							pbs_attr->set_attr( pbs_attr, "Shell_Path_List" , arg );
							break;
						case 'u' :
							pbs_attr->set_attr( pbs_attr, "User_List" , arg );
							break;
						case 'v' :
							pbs_attr->set_attr( pbs_attr, "Variable_List" , arg );
							break;
						case 'M' :
							pbs_attr->set_attr( pbs_attr, "Mail_Users" , arg );
							break;
						case 'l' :
							parse_resources(pbs_attr, arg);
							break;							
						default :
							
							fsd_exc_raise_fmt(FSD_DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE,
									"Invalid native specification: %s (Unsupported option: -%c)",
									native_specification, opt);
					}
					opt = 0;
				}
			}

			if (opt) /* option without optarg */
				fsd_exc_raise_fmt(FSD_DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE,
						"Invalid native specification: %s",
						native_specification);

		 }
		FINALLY
		 {
#ifndef PBS_PROFESSIONAL
			pbs_attr->set_attr( pbs_attr, "submit_args", native_specification);
#endif
			args_list->destroy(args_list);
			fsd_free(native_spec_copy);
		 }
		END_TRY
	}
}

