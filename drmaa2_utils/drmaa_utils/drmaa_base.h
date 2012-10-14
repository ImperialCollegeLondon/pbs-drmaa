/* $Id: drmaa_base.h 13 2011-04-20 15:41:43Z mmamonski $ */
/*
 *  PSNC DRMAA 2.0 utilities library
 *  Copyright (C) 2012  Poznan Supercomputing and Networking Center
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __DRMAA_UTILS__DRMAA_BASE_H
#define __DRMAA_UTILS__DRMAA_BASE_H

#ifdef HAVE_CONFIG_H
#	include <config.h>
#endif

#include <drmaa_utils/common.h>
#include <drmaa_utils/drmaa2.h>


struct fsd_drmaa_singletone_s {

	fsd_drmaa_jsession_t *(*new_jsession)(fsd_drmaa_singletone_t *self, const char *contact);
	fsd_drmaa_rsession_t *(*new_rsession)(fsd_drmaa_singletone_t *self, const char *contact);
	fsd_drmaa_msession_t *(*new_msession)(fsd_drmaa_singletone_t *self, const char *contact);

	drmaa2_string (*get_drms_name)(void);
	drmaa2_version (*get_drms_version)(void);
	drmaa2_string (*get_drmaa_name)(void);
	drmaa2_version (*get_drmaa_version)(void);
	drmaa2_bool (*drmaa2_supports)(const drmaa2_capability c);
};

extern fsd_drmaa_singletone_t _fsd_drmaa_singletone;

#endif /* __DRMAA_UTILS__DRMAA_BASE_H */
