/* $Id: drmaa_base.c 13 2011-04-20 15:41:43Z mmamonski $ */
/*
 *  FedStage DRMAA utilities library
 *  Copyright (C) 2006-2008  FedStage Systems
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

/**
 * @file drmaa_base.c
 * DRM independant part of DRMAA library.
 */

#ifdef HAVE_CONFIG_H
#	include <config.h>
#endif

#include <stdlib.h>
#include <string.h>

#include <drmaa_utils/drmaa2.h>

#ifndef lint
static char rcsid[]
#	ifdef __GNUC__
		__attribute__ ((unused))
#	endif
	= "$Id: drmaa_base.c 13 2011-04-20 15:41:43Z mmamonski $";
#endif


#define DRMAA_API_BEGIN \
	volatile int rc = -1; \
	TRY \
	 {
#define DRMAA_API_END \
		rc = DRMAA_ERRNO_SUCCESS; \
	 } \
	EXCEPT_DEFAULT \
	 { \
		const fsd_exc_t *e = fsd_exc_get(); \
		rc = fsd_drmaa_code( e->code(e) ); \
		fsd_log_return(( "=%d: %s", rc, e->message(e) )); \
	 } \
	END_TRY \
	if( rc < 0 ) \
	 { \
		rc = DRMAA_ERRNO_NO_MEMORY; \
	 } \
	return rc;


#define DRMAA_API_BEGIN_PTR \
	volatile int rc = -1; \
	TRY \
	 {
#define DRMAA_API_END_PTR \
		rc = DRMAA_ERRNO_SUCCESS; \
	 } \
	EXCEPT_DEFAULT \
	 { \
		const fsd_exc_t *e = fsd_exc_get(); \
		rc = fsd_drmaa_code( e->code(e) ); \
		fsd_log_return(( "=%d: %s", rc, e->message(e) )); \
	 } \
	END_TRY \
	if( rc < 0 ) \
	 { \
		rc = DRMAA_ERRNO_NO_MEMORY; \
	 } \
	return NULL;



void drmaa2_string_free(drmaa2_string *ptr)
{

}

drmaa2_error
drmaa2_lasterror(void)
{

}

drmaa2_string
drmaa2_lasterror_text(void)
{

}

drmaa2_list
drmaa2_list_create(const drmaa2_listtype t, const drmaa2_list_entryfree callback)
{

}

void
drmaa2_list_free(drmaa2_list * l)
{


}

const void *
drmaa2_list_get(const drmaa2_list l, long pos)
{

}

drmaa2_error
drmaa2_list_add(drmaa2_list l, const void * value)
{

}

drmaa2_error
drmaa2_list_del(drmaa2_list l, long pos)
{

}

long
drmaa2_list_size(const drmaa2_list l)
{


}


drmaa2_dict
drmaa2_dict_create (const drmaa2_dict_entryfree free_callback)
{


}

void drmaa2_dict_free(drmaa2_dict * d)
{

}

drmaa2_string_list
drmaa2_dict_list(const drmaa2_dict d)
{

}

drmaa2_bool
drmaa2_dict_has(const drmaa2_dict d, const char * key)
{

}

const char *
drmaa2_dict_get(const drmaa2_dict d, const char * key)
{

}

drmaa2_error
drmaa2_dict_del(drmaa2_dict d, const char * key)
{


}

drmaa2_error
drmaa2_dict_set(drmaa2_dict d, const char * key, const char * val)
{

}


drmaa2_jinfo
drmaa2_jinfo_create(void)
{

}

void
drmaa2_jinfo_free(drmaa2_jinfo * ji)
{

}

void drmaa2_slotinfo_free(drmaa2_slotinfo * si)
{

}


void drmaa2_rinfo_free(drmaa2_rinfo * ri)
{

}

drmaa2_jtemplate
drmaa2_jtemplate_create(void)
{

}


void
drmaa2_jtemplate_free(drmaa2_jtemplate * jt)
{

}


drmaa2_rtemplate
drmaa2_rtemplate_create(void)
{

}

void
drmaa2_rtemplate_free(drmaa2_rtemplate * rt)
{

}

void
drmaa2_notification_free(drmaa2_notification * n)
{

}

void
drmaa2_queueinfo_free(drmaa2_queueinfo * qi)
{

}

void
drmaa2_version_free(drmaa2_version * v)
{

}


void
drmaa2_machineinfo_free(drmaa2_machineinfo * mi)
{

}

drmaa2_string_list
drmaa2_jtemplate_impl_spec(void)
{

}

drmaa2_string_list
drmaa2_jinfo_impl_spec(void)
{

}

drmaa2_string_list
drmaa2_rtemplate_impl_spec(void)
{

}

drmaa2_string_list
drmaa2_rinfo_impl_spec(void)
{

}

drmaa2_string_list
drmaa2_queueinfo_impl_spec(void)
{


}

drmaa2_string_list
drmaa2_machineinfo_impl_spec(void)
{


}

drmaa2_string_list
drmaa2_notification_impl_spec(void)
{

}

drmaa2_string
drmaa2_get_instance_value(const void * instance, const char * name)
{

}

drmaa2_string
drmaa2_describe_attribute(const void * instance, const char * name)
{

}

drmaa2_error
drmaa2_set_instance_value(void * instance, const char * name, const char * value)
{

}

void
drmaa2_jsession_free(drmaa2_jsession * js)
{

}

void drmaa2_rsession_free(drmaa2_rsession * rs)
{

}

void
drmaa2_msession_free(drmaa2_msession * ms)
{

}

void
drmaa2_j_free(drmaa2_j * j)
{

}

void
drmaa2_jarray_free(drmaa2_jarray * ja)
{


}

void drmaa2_r_free(drmaa2_r * r)
{

}

drmaa2_string
drmaa2_rsession_get_contact(const drmaa2_rsession rs)
{

}

drmaa2_string
drmaa2_rsession_get_session_name(const drmaa2_rsession rs)
{

}

drmaa2_r
drmaa2_rsession_get_reservation(const drmaa2_rsession rs, const drmaa2_string reservationId)
{

}

drmaa2_r
drmaa2_rsession_request_reservation(const drmaa2_rsession rs, const drmaa2_rtemplate rt)
{

}

drmaa2_r_list
drmaa2_rsession_get_reservations(const drmaa2_rsession rs)
{

}

drmaa2_string
drmaa2_r_get_id(const drmaa2_r r)
{

}

drmaa2_string
drmaa2_r_get_session_name(const drmaa2_r r)
{

}

drmaa2_rtemplate
drmaa2_r_get_reservation_template (const drmaa2_r r)
{

}

drmaa2_rinfo
drmaa2_r_get_info(const drmaa2_r r)
{

}

drmaa2_error
drmaa2_r_terminate(drmaa2_r r)
{

}

drmaa2_string
drmaa2_jarray_get_id(const drmaa2_jarray ja)
{

}

drmaa2_j_list
drmaa2_jarray_get_jobs(const drmaa2_jarray ja)
{

}

drmaa2_string
drmaa2_jarray_get_session_name(const drmaa2_jarray ja)
{

}

drmaa2_jtemplate
drmaa2_jarray_get_job_template(const drmaa2_jarray ja)
{

}

drmaa2_error drmaa2_jarray_suspend(drmaa2_jarray ja)
{

}

drmaa2_error drmaa2_jarray_resume(drmaa2_jarray ja)
{

}

drmaa2_error drmaa2_jarray_hold(drmaa2_jarray ja)
{

}

drmaa2_error drmaa2_jarray_release(drmaa2_jarray ja)
{

}

drmaa2_error
drmaa2_jarray_terminate(drmaa2_jarray ja)
{

}

drmaa2_string
drmaa2_jsession_get_contact(const drmaa2_jsession js)
{

}


drmaa2_string drmaa2_jsession_get_session_name(const drmaa2_jsession js)
{

}

drmaa2_string_list
drmaa2_jsession_get_job_categories(const drmaa2_jsession js)
{

}

drmaa2_j_list
drmaa2_jsession_get_jobs(const drmaa2_jsession js, const drmaa2_jinfo filter)
{


}

drmaa2_jarray
drmaa2_jsession_get_job_array(const drmaa2_jsession js, const drmaa2_string jobarrayId)
{

}

drmaa2_j
drmaa2_jsession_run_job(const drmaa2_jsession js, const drmaa2_jtemplate jt)
{

}

drmaa2_jarray
drmaa2_jsession_run_bulk_jobs(const drmaa2_jsession js, const drmaa2_jtemplate jt, unsigned long begin_index, unsigned long end_index, unsigned long step, unsigned long max_parallel)
{

}

drmaa2_j
drmaa2_jsession_wait_any_started(const drmaa2_jsession js, const drmaa2_j_list l, const time_t timeout)
{

}

drmaa2_j drmaa2_jsession_wait_any_terminated(const drmaa2_jsession js, const drmaa2_j_list l, const time_t timeout)
{

}

drmaa2_string
drmaa2_j_get_id(const drmaa2_j j)
{

}

drmaa2_string
drmaa2_j_get_session_name(const drmaa2_j j)
{

}

drmaa2_jtemplate
drmaa2_j_get_jt(const drmaa2_j j)
{

}

drmaa2_error
drmaa2_j_suspend(drmaa2_j j)
{

}

drmaa2_error
drmaa2_j_resume(drmaa2_j j)
{

}

drmaa2_error
drmaa2_j_hold(drmaa2_j j)
{

}

drmaa2_error
drmaa2_j_release(drmaa2_j j)
{
}

drmaa2_error
drmaa2_j_terminate(drmaa2_j j)
{
}

drmaa2_jstate
drmaa2_j_get_state(const drmaa2_j j, drmaa2_string * substate)
{
}

drmaa2_jinfo
drmaa2_j_get_info(const drmaa2_j j)
{
}

drmaa2_error
drmaa2_j_wait_started(const drmaa2_j j, const time_t timeout)
{
}

drmaa2_error
drmaa2_j_wait_terminated(const drmaa2_j j, const time_t timeout)
{
}


drmaa2_r_list
drmaa2_msession_get_all_reservations(const drmaa2_msession ms)
{
}

drmaa2_j_list
drmaa2_msession_get_all_jobs(const drmaa2_msession ms, const drmaa2_jinfo filter)
{
}

drmaa2_queueinfo_list
drmaa2_msession_get_all_queues(const drmaa2_msession ms, const drmaa2_string_list names)
{
}

drmaa2_machineinfo_list
drmaa2_msession_get_all_machines(const drmaa2_msession ms, const drmaa2_string_list names)
{
}

drmaa2_string
drmaa2_get_drms_name (void)
{
}

drmaa2_version
drmaa2_get_drms_version (void)
{
}

drmaa2_string
drmaa2_get_drmaa_name (void)
{
}

drmaa2_version
drmaa2_get_drmaa_version (void)
{
}

drmaa2_bool
drmaa2_supports (const drmaa2_capability c)
{
}

drmaa2_jsession
drmaa2_create_jsession(const char * session_name, const char * contact)
{
}

drmaa2_rsession
drmaa2_create_rsession(const char * session_name, const char * contact)
{
}

drmaa2_jsession
drmaa2_open_jsession(const char * session_name)
{
}

drmaa2_rsession
drmaa2_open_rsession(const char * session_name)
{
}

drmaa2_msession
drmaa2_open_msession(const char * session_name)
{
}

drmaa2_error
drmaa2_close_jsession(drmaa2_jsession js)
{
}

drmaa2_error
drmaa2_close_rsession(drmaa2_rsession rs)
{
}

drmaa2_error
drmaa2_close_msession(drmaa2_msession ms)
{
}

drmaa2_error
drmaa2_destroy_jsession(const char * session_name)
{
}

drmaa2_error
drmaa2_destroy_rsession(const char * session_name)
{
}

drmaa2_string_list
drmaa2_get_jsession_names(void)
{
}

drmaa2_string_list
drmaa2_get_rsession_names(void)
{
}

drmaa2_error
drmaa2_register_event_notification(const drmaa2_callback callback)
{
}




#if 0

int
drmaa_init(
		const char *contact,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_drmaa_singletone_t *global = &_fsd_drmaa_singletone;
	fsd_log_enter(( "(contact=%s)", contact ));

	fsd_mutex_lock( &global->session_mutex );
	TRY
	 {
		if( global->session != NULL )
			fsd_exc_raise_code( FSD_DRMAA_ERRNO_ALREADY_ACTIVE_SESSION );
		global->session = global->new_session( global, contact );
	 }
	FINALLY
	 { fsd_mutex_unlock( &global->session_mutex ); }
	END_TRY

	fsd_log_return(( " =0" ));
	DRMAA_API_END
}


int
drmaa_exit( char *error_diagnosis, size_t error_diag_len )
{
	DRMAA_API_BEGIN
	fsd_drmaa_singletone_t *global = &_fsd_drmaa_singletone;
	fsd_log_enter(( "()" ));

	fsd_mutex_lock( &global->session_mutex );
	TRY
	 {
		if( global->session != NULL )
		 {
			global->session->destroy( global->session );
			global->session = NULL;
		 }
		else
		 {
			fsd_exc_raise_code( FSD_DRMAA_ERRNO_NO_ACTIVE_SESSION );
		 }
	 }
	FINALLY
	 { fsd_mutex_unlock( &global->session_mutex ); }
	END_TRY

	fsd_log_return(( " =0" ));
	DRMAA_API_END
}


int
drmaa_allocate_job_template(
		drmaa_job_template_t **p_jt,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_drmaa_singletone_t *global = &_fsd_drmaa_singletone;
	fsd_log_enter((""));
	*p_jt = (drmaa_job_template_t*)global->new_job_template( global );
	fsd_log_return(( " =0: jt=%p", (void*)*p_jt ));
	DRMAA_API_END
}


int
drmaa_delete_job_template(
		drmaa_job_template_t *drmaa_jt,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_template_t *jt = (fsd_template_t*)drmaa_jt;
	fsd_log_enter(( "(%p)", (void*)jt ));
	if( jt != NULL )
		jt->destroy( jt );
	fsd_log_return(( " =0" ));
	DRMAA_API_END
}


int
drmaa_set_attribute(
		drmaa_job_template_t *drmaa_jt,
		const char *name, const char *value,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_template_t *jt = (fsd_template_t*)drmaa_jt;
	fsd_log_enter(( "(jt=%p, name='%s', value='%s')",
				(void*)drmaa_jt, name, value ));
	if( jt != NULL  &&  name != NULL )
		jt->set_attr( jt, name, value );
	else
		fsd_exc_raise_code( FSD_ERRNO_INVALID_ARGUMENT );
	fsd_log_return(( " =0" ));
	DRMAA_API_END
}


int
drmaa_get_attribute(
	drmaa_job_template_t *drmaa_jt,
	const char *name, char *value, size_t value_len,
	char *error_diagnosis, size_t error_diag_len
	)
{
	DRMAA_API_BEGIN
	fsd_template_t *jt = (fsd_template_t*)drmaa_jt;
	const char *v = NULL;
	fsd_log_enter(( "(jt=%p, name='%s')", (void*)drmaa_jt, name ));
	if( jt == NULL || name == NULL || value == NULL )
		fsd_exc_raise_code( FSD_ERRNO_INVALID_ARGUMENT );
	v = jt->get_attr( jt, name );
	if( v == NULL )
		v = "";
	strlcpy( value, v, value_len );
	fsd_log_return(( " =0: '%s'", value ));
	DRMAA_API_END
}


int
drmaa_set_vector_attribute(
		drmaa_job_template_t *drmaa_jt,
		const char *name, const char *value[],
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_template_t *jt = (fsd_template_t*)drmaa_jt;
	fsd_log_enter(( "(jt=%p, name='%s', value=...)",
				(void*)drmaa_jt, name ));
	if( jt == NULL || name == NULL )
		fsd_exc_raise_code( FSD_ERRNO_INVALID_ARGUMENT );
	jt->set_v_attr( jt, name, value );
	fsd_log_return(( " =0" ));
	DRMAA_API_END
}


int
drmaa_get_vector_attribute(
		drmaa_job_template_t *drmaa_jt,
		const char *name, drmaa_attr_values_t **out_values,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_template_t *jt = (fsd_template_t*)drmaa_jt;

	fsd_log_enter(( "(jt=%p, name='%s')", (void*)drmaa_jt, name ));
	if( jt == NULL || name == NULL || out_values == NULL )
		fsd_exc_raise_code( FSD_ERRNO_INVALID_ARGUMENT );
	*out_values = NULL;
	*out_values = (drmaa_attr_values_t*)fsd_iter_new(
			fsd_copy_vector( jt->get_v_attr( jt, name ) ), -1 );
	fsd_log_return(( " =0" ));

	DRMAA_API_END
}


int
drmaa_get_attribute_names(
		drmaa_attr_names_t **values,
		char *error_diagnosis, size_t error_diag_len )
{
	DRMAA_API_BEGIN
	fsd_drmaa_singletone_t *global = &_fsd_drmaa_singletone;
	*values = (drmaa_attr_names_t*)global->get_attribute_names( global );
	DRMAA_API_END
}


int
drmaa_get_vector_attribute_names(
		drmaa_attr_names_t **values,
		char *error_diagnosis, size_t error_diag_len )
{
	DRMAA_API_BEGIN
	fsd_drmaa_singletone_t *global = &_fsd_drmaa_singletone;
	*values = (drmaa_attr_names_t*)global->get_vector_attribute_names( global );
	DRMAA_API_END
}


#define iter_function(name, type) \
int drmaa_get_next_##name( type *values, char *value, size_t value_len ) \
{ \
	char error_diagnosis[1]; \
	size_t error_diag_len = sizeof(error_diagnosis); \
	DRMAA_API_BEGIN \
	fsd_iter_t *iter = (fsd_iter_t*)values; \
	strlcpy( value, iter->next(iter), value_len ); \
	DRMAA_API_END \
} \
int drmaa_get_num_##name##s( type *values, size_t *size ) \
{ \
	char error_diagnosis[1]; \
	size_t error_diag_len = sizeof(error_diagnosis); \
	DRMAA_API_BEGIN \
	fsd_iter_t *iter = (fsd_iter_t*)values; \
	*size = iter->len(iter); \
	DRMAA_API_END \
} \
void drmaa_release_##name##s( type *values ) \
{ \
	fsd_iter_t *iter = (fsd_iter_t*)values; \
	iter->destroy(iter); \
}

iter_function(attr_name, drmaa_attr_names_t)
iter_function(attr_value, drmaa_attr_values_t)
iter_function(job_id, drmaa_job_ids_t)

#undef iter_function


int
drmaa_control(
		const char *job_id, int action,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_drmaa_session_t *volatile session = NULL;

	fsd_log_enter(( "(job_id=%s, action=%s)",
				job_id, drmaa_control_to_str(action) ));

	if( job_id == NULL )
		fsd_exc_raise_code( FSD_ERRNO_INVALID_ARGUMENT );

	TRY
	 {
		session = fsd_drmaa_session_get();
		session->control_job( session, job_id, action );
	 }
	FINALLY
	 {
		if( session )
			session->release( session );
	 }
	END_TRY

	fsd_log_return(( " =0" ));
	DRMAA_API_END
}



int
drmaa_job_ps(
		const char *job_id, int *remote_ps,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_drmaa_session_t *volatile session = NULL;

	fsd_log_enter(( "(job_id=%s)", job_id ));
	if( job_id == NULL )
		fsd_exc_raise_code( FSD_ERRNO_INVALID_ARGUMENT );

	TRY
	 {
		session = fsd_drmaa_session_get();
		session->job_ps( session, job_id, remote_ps );
	 }
	FINALLY
	 {
		if( session )
			session->release( session );
	 }
	END_TRY

	fsd_log_return(( "(job_id=%s) =0: remote_ps=%s (0x%02x)",
				job_id, drmaa_job_ps_to_str(*remote_ps), *remote_ps ));
	DRMAA_API_END
}



int
drmaa_run_job(
		char *job_id, size_t job_id_len,
		const drmaa_job_template_t *jt,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_drmaa_session_t *volatile session = NULL;
	char *volatile job_id_buf = NULL;
	fsd_log_enter(( "(jt=%p)", (void*)jt ));
	TRY
	 {
		session = fsd_drmaa_session_get();
		job_id_buf = session->run_job( session, (fsd_template_t*)jt ),
		strlcpy( job_id, job_id_buf, job_id_len );
	 }
	FINALLY
	 {
		if( session )
			session->release( session );
		fsd_free( job_id_buf );
	 }
	END_TRY

	fsd_log_return(( " =0: job_id=%s", job_id ));
	DRMAA_API_END
}


int
drmaa_run_bulk_jobs(
		drmaa_job_ids_t **job_ids,
		const drmaa_job_template_t *jt,
		int start, int end, int incr,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_drmaa_session_t *volatile session = NULL;
	fsd_log_enter(( "(jt=%p, start=%d, end=%d, incr=%d)",
		(void*)jt, start, end, incr ));
	TRY
	 {
		if( incr > 0 )
		 {
			if( !(0 < start  &&  start <= end) )
				fsd_exc_raise_code( FSD_ERRNO_INVALID_ARGUMENT );
		 }
		else if( incr < 0 )
		 {
			if( !(start >= end  &&  end > 0) )
				fsd_exc_raise_code( FSD_ERRNO_INVALID_ARGUMENT );
		 }
		else
			fsd_exc_raise_code( FSD_ERRNO_INVALID_ARGUMENT );

		session = fsd_drmaa_session_get();
		*job_ids = (drmaa_job_ids_t*)session->run_bulk(
				session, (fsd_template_t*)jt,
				start, end, incr
				);
	 }
	EXCEPT_DEFAULT
	 {
		*job_ids = NULL;
		fsd_exc_reraise();
	 }
	FINALLY
	 {
		if( session )
			session->release( session );
	 }
	END_TRY
	fsd_log_return(( " =0" ));
	DRMAA_API_END
}



static struct timespec *
drmaa_timeout_time( signed long timeout, struct timespec *ts )
{
	if( timeout == DRMAA_TIMEOUT_WAIT_FOREVER )
		return NULL;
	else
	 {
		fsd_get_time( ts );
		if( timeout != DRMAA_TIMEOUT_NO_WAIT )
			ts->tv_sec += timeout;
		return ts;
	 }
}


int
drmaa_synchronize(
		const char **job_ids, signed long timeout,
		int dispose,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_drmaa_session_t *volatile session = NULL;
	struct timespec ts;

	fsd_log_enter(( "(job_ids={...}, timeout=%ld, dispose=%d)",
			timeout, dispose ));

	if( job_ids == NULL )
		fsd_exc_raise_code( FSD_ERRNO_INVALID_ARGUMENT );

	TRY
	 {
		session = fsd_drmaa_session_get();
		session->synchronize( session, job_ids,
				drmaa_timeout_time(timeout, &ts), dispose );
	 }
	FINALLY
	 {
		if( session )
			session->release( session );
	 }
	END_TRY

	fsd_log_return(( " =0" ));
	DRMAA_API_END
}


int
drmaa_wait(
		const char *job_id, char *job_id_out, size_t job_id_out_len,
		int *stat, signed long timeout, drmaa_attr_values_t **rusage,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_drmaa_session_t *volatile session = NULL;
	struct timespec ts;
	char *result_job_id = NULL;

	fsd_log_enter(( "(job_id=%s, timeout=%ld)", job_id, timeout ));

	TRY
	 {
		session = fsd_drmaa_session_get();
		result_job_id = session->wait(
				session, job_id, drmaa_timeout_time(timeout, &ts),
				stat, (fsd_iter_t**)rusage
				);
		strlcpy( job_id_out, result_job_id, job_id_out_len );
	 }
	FINALLY
	 {
		fsd_free( result_job_id );
		if( session )
			session->release( session );
	 }
	END_TRY

	fsd_log_return(( " =0: job_id=%s", job_id_out ));
	DRMAA_API_END
}


#if 0
int
drmaa_get_contact(
		char *contact, size_t contact_len,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	drmaa_session_t *volatile session = NULL;
	const drmaa_implementation_info_t *impl_info = NULL;
	const char *result = NULL;

	impl_info = drmaa_get_implementation_info();
	TRY
	 {
		session = drmaa_session_get();
		if( session->contact )
			result = session->contact;
		else
			result = impl_info->default_contact;
		strlcpy( contact, result, contact_len );
	 }
	EXCEPT( FSD_DRMAA_ERRNO_NO_ACTIVE_SESSION )
	 {
		result = impl_info->default_contact;
		strlcpy( contact, result, contact_len );
	 }
	FINALLY
	 {
		drmaa_session_release( session );
	 }
	END_TRY

	DRMAA_API_END
}
#endif


int
drmaa_get_contact(
		char *contact, size_t contact_len,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_drmaa_singletone_t *global = &_fsd_drmaa_singletone;
	fsd_log_enter(( "" ));
	strlcpy( contact, global->get_contact( global ), contact_len );
	fsd_log_return(( " =0: %s", contact ));
	DRMAA_API_END
}


int
drmaa_version(
		unsigned int *major, unsigned int *minor,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_drmaa_singletone_t *global = &_fsd_drmaa_singletone;
	fsd_log_enter(( "" ));
	global->get_version( global, major, minor );
	fsd_log_return(( " =0: %d.%d", *major, *minor ));
	DRMAA_API_END
}


int
drmaa_get_DRM_system(
		char *drm_system, size_t drm_system_len,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_drmaa_singletone_t *global = &_fsd_drmaa_singletone;
	fsd_log_enter(( "" ));
	strlcpy( drm_system, global->get_DRM_system( global ), drm_system_len );
	fsd_log_return(( " =0: %s", drm_system ));
	DRMAA_API_END
}


int
drmaa_get_DRMAA_implementation(
		char *drmaa_impl, size_t drmaa_impl_len,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_drmaa_singletone_t *global = &_fsd_drmaa_singletone;
	fsd_log_enter(( "" ));
	strlcpy( drmaa_impl, global->get_DRMAA_implementation( global ), 
			drmaa_impl_len );
	fsd_log_return(( " =0: %s", drmaa_impl ));
	DRMAA_API_END
}


const char *
drmaa_strerror( int drmaa_errno )
{
	return fsd_drmaa_strerror( drmaa_errno );
}


int
drmaa_read_configuration_file(
		const char *filename, int must_exist,
		char *error_diagnosis, size_t error_diag_len
		)
{
	fsd_drmaa_session_t *volatile session = NULL;

	DRMAA_API_BEGIN
	fsd_log_enter(( "(filename=%s)", filename ));
	TRY
	 {
		session = fsd_drmaa_session_get();
		session->read_configuration(
				session, filename, must_exist, NULL, 0 );
	 }
	FINALLY
	 {
		if( session )
			session->release( session );
	 }
	END_TRY

	fsd_log_return(( " =0" ));
	DRMAA_API_END
}


int
drmaa_read_configuration(
		const char *configuration, size_t configuration_len,
		char *error_diagnosis, size_t error_diag_len
		)
{
	DRMAA_API_BEGIN
	fsd_drmaa_session_t *volatile session = NULL;
	fsd_log_enter(( "(configuration=\"%.*s\")",
				(int)configuration_len, configuration ));
	TRY
	 {
		session = fsd_drmaa_session_get();
		session->read_configuration(
				session, NULL, false, configuration, configuration_len );
	 }
	FINALLY
	 {
		if( session )
			session->release( session );
	 }
	END_TRY
	fsd_log_return(( " =0" ));
	DRMAA_API_END
}



fsd_drmaa_session_t *
fsd_drmaa_session_get(void)
{
	fsd_drmaa_session_t *self = NULL;
	fsd_mutex_lock( &_fsd_drmaa_singletone.session_mutex );
	self = _fsd_drmaa_singletone.session;
	fsd_mutex_unlock( &_fsd_drmaa_singletone.session_mutex );
	if( self != NULL )
	 {
		fsd_mutex_lock( &self->mutex );
		self->ref_cnt ++;
		fsd_mutex_unlock( &self->mutex );
	 }
	else
		fsd_exc_raise_code( FSD_DRMAA_ERRNO_NO_ACTIVE_SESSION );
	return self;
}


#define wiffunc( name, args_decl, args_list ) \
int drmaa_##name args_decl \
{ \
	return _fsd_drmaa_singletone.name args_list;\
}

wiffunc(
		wifexited,
		(	int *exited, int stat, char *error_diagnosis, size_t error_diag_len ),
		( exited, stat, error_diagnosis, error_diag_len )
		)

wiffunc(
		wexitstatus,
		(	int *exit_status, int stat,
			char *error_diagnosis, size_t error_diag_len ),
		( exit_status, stat, error_diagnosis, error_diag_len )
		)

wiffunc(
		wifsignaled,
		(	int *signaled, int stat, char *error_diagnosis, size_t error_diag_len ),
		( signaled, stat, error_diagnosis, error_diag_len )
		)

wiffunc(
		wtermsig,
		(	char *signal, size_t signal_len, int stat,
			char *error_diagnosis, size_t error_diag_len ),
		( signal, signal_len, stat, error_diagnosis, error_diag_len )
		)

wiffunc(
		wcoredump,
		(	int *core_dumped, int stat,
			char *error_diagnosis, size_t error_diag_len ),
		( core_dumped, stat, error_diagnosis, error_diag_len )
		)

wiffunc(
		wifaborted,
		(	int *aborted, int stat, char *error_diagnosis, size_t error_diag_len ),
		( aborted, stat, error_diagnosis, error_diag_len )
		)

#endif
