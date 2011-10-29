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

#ifndef __PBS_DRMAA__UTIL_H
#define __PBS_DRMAA__UTIL_H

#ifdef HAVE_CONFIG_H
#	include <config.h>
#endif

void pbsdrmaa_exc_raise_pbs( const char *function );
int pbsdrmaa_map_pbs_errno( int _pbs_errno );

struct attrl;

void pbsdrmaa_free_attrl( struct attrl *list );
void pbsdrmaa_dump_attrl(
		const struct attrl *attribute_list, const char *prefix );

/**
 * Writes temporary file.
 * @param content   Buffer with content to write.
 * @param len       Buffer's length.
 * @return Path to temporary file.
 */
char *
pbsdrmaa_write_tmpfile( const char *content, size_t len );

ssize_t
fsd_getline(char * line,ssize_t size, int fd);

ssize_t 
fsd_getline_buffered(char * line,char * buf, ssize_t size, int fd, int * idx, int * end_idx, int * line_idx);

#endif /* __PBS_DRMAA__UTIL_H */

