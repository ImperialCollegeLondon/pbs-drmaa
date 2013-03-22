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
#include <stdlib.h>

#include <pbs_ifl.h>
#include <pbs_error.h>

#include <drmaa_utils/conf.h>
#include <drmaa_utils/exec.h>
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

static void pbsdrmaa_submit_destroy( pbsdrmaa_submit_t *self );

static char *pbsdrmaa_submit_submit( pbsdrmaa_submit_t *self );

static void pbsdrmaa_submit_eval( pbsdrmaa_submit_t *self );

static void pbsdrmaa_submit_set( pbsdrmaa_submit_t *self, const char *pbs_attr, char *value, unsigned placeholders );

static struct attrl *pbsdrmaa_submit_filter(struct attrl *pbs_attr);


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
		self->apply_native_specification = pbsdrmaa_submit_apply_native_specification;

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
	struct attrl *volatile pbs_attr = NULL;
	char *volatile job_id = NULL;
	TRY
	 {
		fsd_template_t *pbs_tmpl = self->pbs_job_attributes;
		int i;

		for( i = PBSDRMAA_N_PBS_ATTRIBUTES - 1; i >= 0; i-- ) /* down loop -> start with custom resources */
		 {
			const char *name = pbs_tmpl->by_code( pbs_tmpl, i )->name;
			const char *value = pbs_tmpl->get_attr( pbs_tmpl, name );

			if (!value)
				continue;

			if ( i == PBSDRMAA_ATTR_CUSTOM_RESOURCES)
			 {
				char *value_copy = fsd_strdup(value);
				char *tok_comma_ctx = NULL;
				char *res_token = NULL;
				/* matlab:2,simulink:1 */

				for (res_token = strtok_r(value_copy, ";", &tok_comma_ctx); res_token; res_token = strtok_r(NULL, ";", &tok_comma_ctx))
				 {
					char *value_p = strstr(res_token, ":");

					if (value_p)
					 {
						char *name_p = NULL;
						*value_p = '\0';
						value_p++;
						name_p = fsd_asprintf("Resource_List.%s",res_token);
						pbs_attr = pbsdrmaa_add_attr( pbs_attr, name_p, value_p );
						fsd_free(name_p);
					 }
					else
					 {
						fsd_exc_raise_code( FSD_DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE );
					 }
				 }

				fsd_free(value_copy);
			 }
			else if (i == PBSDRMAA_ATTR_NODE_PROPERTIES)
			 {
				const char *nodes_value = pbs_tmpl->get_attr( pbs_tmpl, PBSDRMAA_NODES );
				char *final_value = NULL;

				if (nodes_value)
				 {
					final_value = fsd_asprintf("%s:%s",nodes_value, value);
				 }
				else
				 {
					final_value = fsd_asprintf("1:%s", value);
				 }

				pbs_tmpl->set_attr( pbs_tmpl, PBSDRMAA_NODES, final_value);
				fsd_free(final_value);
			 }
			else
			 {
				pbs_attr = pbsdrmaa_add_attr( pbs_attr, name, value );
			 }
		 }


		pbs_attr = pbsdrmaa_submit_filter(pbs_attr);

		job_id = ((pbsdrmaa_session_t *)self->session)->pbs_connection->submit( ((pbsdrmaa_session_t *)self->session)->pbs_connection, (struct attropl*)pbs_attr, self->script_filename, self->destination_queue);

	 }
	EXCEPT_DEFAULT
	 {
		fsd_free( job_id );
		fsd_exc_reraise();
	 }
	FINALLY
	 {
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
		script_len += strlen("touch ") + strlen(((pbsdrmaa_session_t *)self->session)->job_exit_status_file_prefix) + strlen("/$PBS_JOBID.started;");
		script_len += strlen(executable);
		if( argv != NULL )
			for( i = argv;  *i != NULL;  i++ )
				script_len += 3+strlen(*i);
		if( input_path != NULL )
			script_len += strlen(" <") + strlen(input_path);

		script_len += strlen(";EXIT_CODE=$?; echo $EXIT_CODE >") + strlen(((pbsdrmaa_session_t *)self->session)->job_exit_status_file_prefix) + strlen("/$PBS_JOBID.exitcode; exit $EXIT_CODE");
	 }

	fsd_calloc( script, script_len+1, char );

	 {
		char *s;
		s = script;
		if( wd != NULL )
			s += sprintf( s, "cd %s; ", wd );
		s += sprintf( s, "touch %s/$PBS_JOBID.started;", ((pbsdrmaa_session_t *)self->session)->job_exit_status_file_prefix);
		s += sprintf( s, "%s", executable );
		if( argv != NULL )
			for( i = argv;  *i != NULL;  i++ )
				s += sprintf( s, " '%s'", *i );
		if( input_path != NULL )
			s += sprintf( s, " <%s", input_path );

		s += sprintf( s, ";EXIT_CODE=$?; echo $EXIT_CODE >%s/$PBS_JOBID.exitcode; exit $EXIT_CODE", ((pbsdrmaa_session_t *)self->session)->job_exit_status_file_prefix);

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
	if (join_files != NULL && strcmp(join_files, "y") == 0)
	  {
		pbs_attr->set_attr( pbs_attr, PBSDRMAA_JOIN_FILES, "oe" ); 
	  }
        else if (join_files != NULL && strcmp(join_files, "n") != 0)
          {
		fsd_exc_raise_fmt( FSD_ERRNO_INVALID_VALUE, "invalid value of %s attribute. Should be 'y' or 'n'.",
					DRMAA_JOIN_FILES );
          }
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
		while (env_v[ii])
		 {
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

static const char *get_job_env(pbsdrmaa_submit_t *self, const char *env_name)
{
	const fsd_template_t *jt = self->job_template;
	const char *const *env_v = jt->get_v_attr( jt, DRMAA_V_ENV);
	int ii = 0;

	while (env_v[ii])
	 {
		char *eq_p = strstr(env_v[ii], "=");

		if ((eq_p) && (strncmp(env_v[ii], env_name, eq_p - env_v[ii]) == 0))
				return ++eq_p;

		ii++;
	 }

	return NULL;
}

static void parse_resources(pbsdrmaa_submit_t *self, fsd_template_t *pbs_attr,const char *resources)
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
				if (value[0] == '$' && get_job_env(self, value + 1))
					pbs_attr->set_attr( pbs_attr, name , get_job_env(self, value + 1) ); /*get value from job env variable */
				else
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
		fsd_template_t *pbs_attr = self->pbs_job_attributes;
		char *arg = NULL;
		volatile char * native_spec_copy = fsd_strdup(native_specification);
		char * ctxt = NULL;
		int opt = 0;

		TRY
		  {
			for (arg = strtok_r((char *)native_spec_copy, " \t", &ctxt); arg; arg = strtok_r(NULL, " \t",&ctxt) ) {
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
							parse_resources( self, pbs_attr, arg);
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
			fsd_free((char *)native_spec_copy);
		 }
		END_TRY
	}
}

struct attrl *
pbsdrmaa_submit_filter(struct attrl *pbs_attr)
{
	fsd_log_enter(( "({pbs_attr=%p})", (void*)pbs_attr));

	if (getenv(PBSDRMAA_SUBMIT_FILTER_ENV) == NULL) 
	  {
		return pbs_attr;
	  } 
	else 
	  {
		struct attrl *ii = NULL;
		char *empty_args[] = { NULL };
		int exit_status = -1;
		const char *submit_filter = getenv(PBSDRMAA_SUBMIT_FILTER_ENV);
		char *stdin_buf = NULL;
		int stdin_size = 0;
		char *stdout_buf = NULL;
		char *stderr_buf = NULL;
		char *output_line = NULL;
		char *ctx = NULL;

		fsd_log_debug(("Executing filter script: %s", submit_filter));
		
		
		for (ii = pbs_attr; ii; ii = ii->next) 
		  {
			stdin_size += strlen(ii->name) + strlen(ii->value) + 2 /* '=' and '\n' */;
			if (ii->resource)
			  {
				stdin_size += strlen(ii->resource) +  1; /* '.' */
			  }
		  }

		stdin_size+=1; /* '\0' */

		stdin_buf = fsd_calloc(stdin_buf, stdin_size, char);
		stdin_buf[0] = '\0';

		for (ii = pbs_attr; ii; ii = ii->next) 
		  {
			strcat(stdin_buf, ii->name);
			if (ii->resource)
			  {
				strcat(stdin_buf, ".");
				strcat(stdin_buf, ii->resource);
			  }
			strcat(stdin_buf, "=");
			strcat(stdin_buf, ii->value);
			strcat(stdin_buf, "\n");
		  }
		
		exit_status = fsd_exec_sync(submit_filter, empty_args, stdin_buf, &stdout_buf, &stderr_buf);
    fsd_free(stdin_buf);

		if (exit_status != 0)
		  {
			fsd_log_error(("Filter script %s exited with non-zero code: %d", submit_filter, exit_status));
			fsd_exc_raise_fmt(FSD_DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE, "Submit filter script failed (code: %d, message: %s)", exit_status, stderr_buf);
		  }
		
		fsd_log_debug(("Submit filter exit_status=%d, stderr=%s", exit_status, stderr_buf));

		pbsdrmaa_free_attrl(pbs_attr);
		pbs_attr = NULL;

		/* exit_status == 0 */
		for (output_line = strtok_r(stdout_buf, "\n", &ctx);  output_line ; output_line = strtok_r(NULL, "\n", &ctx))
		  {
			char *attr_name = NULL;
			char *attr_value = NULL;

			attr_value = strstr(output_line,"=");
			attr_name = output_line;

			if (!attr_value)
			  {
				fsd_exc_raise_fmt(FSD_DRMAA_ERRNO_INVALID_ATTRIBUTE_FORMAT, "Invalid output line of submit filter: %s", output_line);
			  }
			else
			  {
				*attr_value = '\0';
				attr_value++; 
			  }
			

			pbs_attr = pbsdrmaa_add_attr( pbs_attr, attr_name, attr_value );
      }

	  fsd_free(stdout_buf);
    fsd_free(stderr_buf);	

		return pbs_attr;
	  }

}

