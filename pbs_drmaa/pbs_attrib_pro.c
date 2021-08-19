/* ANSI-C code produced by gperf version 3.0.3 */
/* Command-line: gperf --readonly-tables --output-file=pbs_attrib_pro.c pbs_attrib_pro.gperf  */
/* Computed positions: -k'1,3,15-16' */

#if !((' ' == 32) && ('!' == 33) && ('"' == 34) && ('#' == 35) \
      && ('%' == 37) && ('&' == 38) && ('\'' == 39) && ('(' == 40) \
      && (')' == 41) && ('*' == 42) && ('+' == 43) && (',' == 44) \
      && ('-' == 45) && ('.' == 46) && ('/' == 47) && ('0' == 48) \
      && ('1' == 49) && ('2' == 50) && ('3' == 51) && ('4' == 52) \
      && ('5' == 53) && ('6' == 54) && ('7' == 55) && ('8' == 56) \
      && ('9' == 57) && (':' == 58) && (';' == 59) && ('<' == 60) \
      && ('=' == 61) && ('>' == 62) && ('?' == 63) && ('A' == 65) \
      && ('B' == 66) && ('C' == 67) && ('D' == 68) && ('E' == 69) \
      && ('F' == 70) && ('G' == 71) && ('H' == 72) && ('I' == 73) \
      && ('J' == 74) && ('K' == 75) && ('L' == 76) && ('M' == 77) \
      && ('N' == 78) && ('O' == 79) && ('P' == 80) && ('Q' == 81) \
      && ('R' == 82) && ('S' == 83) && ('T' == 84) && ('U' == 85) \
      && ('V' == 86) && ('W' == 87) && ('X' == 88) && ('Y' == 89) \
      && ('Z' == 90) && ('[' == 91) && ('\\' == 92) && (']' == 93) \
      && ('^' == 94) && ('_' == 95) && ('a' == 97) && ('b' == 98) \
      && ('c' == 99) && ('d' == 100) && ('e' == 101) && ('f' == 102) \
      && ('g' == 103) && ('h' == 104) && ('i' == 105) && ('j' == 106) \
      && ('k' == 107) && ('l' == 108) && ('m' == 109) && ('n' == 110) \
      && ('o' == 111) && ('p' == 112) && ('q' == 113) && ('r' == 114) \
      && ('s' == 115) && ('t' == 116) && ('u' == 117) && ('v' == 118) \
      && ('w' == 119) && ('x' == 120) && ('y' == 121) && ('z' == 122) \
      && ('{' == 123) && ('|' == 124) && ('}' == 125) && ('~' == 126))
/* The character set is not based on ISO-646.  */
#error "gperf generated tables don't work with this execution character set. Please report a bug to <bug-gnu-gperf@gnu.org>."
#endif

#line 1 "pbs_attrib_pro.gperf"

/* $Id: pbs_attrib.gperf 71 2012-09-05 20:48:10Z mmamonski $ */
/*
 *  FedStage DRMAA for PBS Pro
 *  Copyright (C) 2006-2009  FedStage Systems
 *  Copyright (C) 2011 Poznan Supercomputing and Networking Center
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

#include <drmaa_utils/drmaa_attrib.h>
#include <drmaa_utils/template.h>
#include <pbs_drmaa/pbs_attrib.h>

#ifndef lint
static char rcsid[]
#	ifdef __GNUC__
		__attribute__ ((unused))
#	endif
	= "$Id: pbs_attrib.gperf 71 2012-09-05 20:48:10Z mmamonski $";
#endif

extern const fsd_attribute_t pbsdrmaa_pbs_attributes[];
#define t(code) \
	( & pbsdrmaa_pbs_attributes[ code ] )
#line 47 "pbs_attrib_pro.gperf"
struct pbs_attrib { int name; const fsd_attribute_t *attr; };
#include <string.h>

#define TOTAL_KEYWORDS 56
#define MIN_WORD_LENGTH 1
#define MAX_WORD_LENGTH 30
#define MIN_HASH_VALUE 1
#define MAX_HASH_VALUE 79
/* maximum key range = 79, duplicates = 0 */

#ifdef __GNUC__
__inline
#else
#ifdef __cplusplus
inline
#endif
#endif
static unsigned int
hash (register const char *str, register unsigned int len)
{
  static const unsigned char asso_values[] =
    {
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80,  0, 80, 45, 80, 50,
      80, 80,  5, 80, 45, 35, 80,  0, 80, 20,
       0, 80, 10, 20, 80, 15,  5, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80,  5,  0, 10,
      10,  0, 15, 10,  0, 15, 25, 80, 50,  0,
       5, 15,  0,  0,  0,  0,  5,  0, 45, 15,
       0, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80, 80, 80, 80, 80,
      80, 80, 80, 80, 80, 80
    };
  register int hval = len;

  switch (hval)
    {
      default:
        hval += asso_values[(unsigned char)str[15]];
      /*FALLTHROUGH*/
      case 15:
        hval += asso_values[(unsigned char)str[14]];
      /*FALLTHROUGH*/
      case 14:
      case 13:
      case 12:
      case 11:
      case 10:
      case 9:
      case 8:
      case 7:
      case 6:
      case 5:
      case 4:
      case 3:
        hval += asso_values[(unsigned char)str[2]];
      /*FALLTHROUGH*/
      case 2:
      case 1:
        hval += asso_values[(unsigned char)str[0]];
        break;
    }
  return hval;
}

struct stringpool_t
  {
    char stringpool_str1[sizeof("x")];
    char stringpool_str5[sizeof("queue")];
    char stringpool_str6[sizeof("server")];
    char stringpool_str9[sizeof("exec_host")];
    char stringpool_str10[sizeof("exec_vnode")];
    char stringpool_str11[sizeof("submit_args")];
    char stringpool_str12[sizeof("stagein")];
    char stringpool_str13[sizeof("stageout")];
    char stringpool_str14[sizeof("resources_used")];
    char stringpool_str15[sizeof("start_time")];
    char stringpool_str16[sizeof("depend")];
    char stringpool_str17[sizeof("comment")];
    char stringpool_str18[sizeof("Variable_List")];
    char stringpool_str19[sizeof("Rerunable")];
    char stringpool_str20[sizeof("mtime")];
    char stringpool_str21[sizeof("destination")];
    char stringpool_str22[sizeof("Account_Name")];
    char stringpool_str23[sizeof("Priority")];
    char stringpool_str24[sizeof("User_List")];
    char stringpool_str25[sizeof("Mail_Users")];
    char stringpool_str26[sizeof("Mail_Points")];
    char stringpool_str27[sizeof("Resource_List.mem")];
    char stringpool_str28[sizeof("Resource_List.pmem")];
    char stringpool_str29[sizeof("Resource_List.procs")];
    char stringpool_str30[sizeof("Resource_List.select")];
    char stringpool_str31[sizeof("interactive")];
    char stringpool_str33[sizeof("Resource_List.arch")];
    char stringpool_str34[sizeof("job_state")];
    char stringpool_str35[sizeof("group_list")];
    char stringpool_str36[sizeof("Output_Path")];
    char stringpool_str37[sizeof("job_array_id")];
    char stringpool_str38[sizeof("Resource_List.cput")];
    char stringpool_str39[sizeof("Resource_List.pcput")];
    char stringpool_str40[sizeof("Shell_Path_List")];
    char stringpool_str42[sizeof("job_array_request")];
    char stringpool_str43[sizeof("Resource_List.host")];
    char stringpool_str44[sizeof("Resource_List.ncpus")];
    char stringpool_str45[sizeof("Keep_Files")];
    char stringpool_str47[sizeof("Resource_List.software")];
    char stringpool_str48[sizeof("Resource_List.nice")];
    char stringpool_str49[sizeof("Resource_List.nodes")];
    char stringpool_str50[sizeof("Resource_List.custom_resources")];
    char stringpool_str52[sizeof("Resource_List.walltime")];
    char stringpool_str53[sizeof("Job_Name")];
    char stringpool_str54[sizeof("Job_Owner")];
    char stringpool_str55[sizeof("Checkpoint")];
    char stringpool_str58[sizeof("Resource_List.file")];
    char stringpool_str59[sizeof("Resource_List.node_properties")];
    char stringpool_str60[sizeof("Error_Path")];
    char stringpool_str64[sizeof("Execution_Time")];
    char stringpool_str65[sizeof("Hold_Types")];
    char stringpool_str69[sizeof("Join_Path")];
    char stringpool_str73[sizeof("Resource_List.vmem")];
    char stringpool_str74[sizeof("Resource_List.pvmem")];
    char stringpool_str76[sizeof("Exit_status")];
    char stringpool_str79[sizeof("Resource_List.place")];
  };
static const struct stringpool_t stringpool_contents =
  {
    "x",
    "queue",
    "server",
    "exec_host",
    "exec_vnode",
    "submit_args",
    "stagein",
    "stageout",
    "resources_used",
    "start_time",
    "depend",
    "comment",
    "Variable_List",
    "Rerunable",
    "mtime",
    "destination",
    "Account_Name",
    "Priority",
    "User_List",
    "Mail_Users",
    "Mail_Points",
    "Resource_List.mem",
    "Resource_List.pmem",
    "Resource_List.procs",
    "Resource_List.select",
    "interactive",
    "Resource_List.arch",
    "job_state",
    "group_list",
    "Output_Path",
    "job_array_id",
    "Resource_List.cput",
    "Resource_List.pcput",
    "Shell_Path_List",
    "job_array_request",
    "Resource_List.host",
    "Resource_List.ncpus",
    "Keep_Files",
    "Resource_List.software",
    "Resource_List.nice",
    "Resource_List.nodes",
    "Resource_List.custom_resources",
    "Resource_List.walltime",
    "Job_Name",
    "Job_Owner",
    "Checkpoint",
    "Resource_List.file",
    "Resource_List.node_properties",
    "Error_Path",
    "Execution_Time",
    "Hold_Types",
    "Join_Path",
    "Resource_List.vmem",
    "Resource_List.pvmem",
    "Exit_status",
    "Resource_List.place"
  };
#define stringpool ((const char *) &stringpool_contents)
#ifdef __GNUC__
__inline
#ifdef __GNUC_STDC_INLINE__
__attribute__ ((__gnu_inline__))
#endif
#endif
const struct pbs_attrib *
pbsdrmaa_pbs_attrib_lookup (register const char *str, register unsigned int len)
{
  static const struct pbs_attrib wordlist[] =
    {
      {-1},
#line 103 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str1, t(PBSDRMAA_ATTR_EXTENSION)},
      {-1}, {-1}, {-1},
#line 98 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str5, t(PBSDRMAA_ATTR_QUEUE)},
#line 99 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str6, t(PBSDRMAA_ATTR_SERVER)},
      {-1}, {-1},
#line 93 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str9, t(PBSDRMAA_ATTR_EXECUTION_HOST)},
#line 94 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str10, t(PBSDRMAA_ATTR_EXECUTION_VNODE)},
#line 104 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str11, t(PBSDRMAA_ATTR_SUBMIT_ARGS)},
#line 90 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str12, t(PBSDRMAA_ATTR_STAGEIN)},
#line 91 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str13, t(PBSDRMAA_ATTR_STAGEOUT)},
#line 96 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str14, t(PBSDRMAA_ATTR_RESOURCES_USED)},
#line 102 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str15, t(PBSDRMAA_ATTR_START_TIME)},
#line 88 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str16, t(PBSDRMAA_ATTR_DEPEND)},
#line 100 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str17, t(PBSDRMAA_ATTR_COMMENT)},
#line 83 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str18, t(PBSDRMAA_ATTR_JOB_ENVIRONMENT)},
#line 79 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str19, t(PBSDRMAA_ATTR_RERUNABLE)},
#line 105 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str20, t(PBSDRMAA_ATTR_MTIME)},
#line 78 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str21, t(PBSDRMAA_ATTR_DESITINATION)},
#line 84 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str22, t(PBSDRMAA_ATTR_ACCOUNT_NAME)},
#line 77 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str23, t(PBSDRMAA_ATTR_PRIORITY)},
#line 82 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str24, t(PBSDRMAA_ATTR_USERS)},
#line 85 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str25, t(PBSDRMAA_ATTR_EMAIL)},
#line 75 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str26, t(PBSDRMAA_ATTR_MAIL_POINTS)},
#line 62 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str27, t(PBSDRMAA_ATTR_MEM_LIMIT)},
#line 64 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str28, t(PBSDRMAA_ATTR_SINGLE_RSS_LIMIT)},
#line 70 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str29, t(PBSDRMAA_ATTR_PROCS)},
#line 74 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str30, t(PBSDRMAA_ATTR_SELECT)},
#line 89 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str31, t(PBSDRMAA_ATTR_INTERACTIVE)},
      {-1},
#line 67 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str33, t(PBSDRMAA_ATTR_ARCHITECTURE)},
#line 97 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str34, t(PBSDRMAA_ATTR_JOB_STATE)},
#line 53 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str35, t(PBSDRMAA_ATTR_GROUPS)},
#line 76 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str36, t(PBSDRMAA_ATTR_OUTPUT_PATH)},
#line 81 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str37, t(PBSDRMAA_ATTR_BULK_IDX)},
#line 58 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str38, t(PBSDRMAA_ATTR_CPU_TIME_LIMIT)},
#line 63 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str39, t(PBSDRMAA_ATTR_SINGLE_CPU_TIME_LIMIT)},
#line 87 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str40, t(PBSDRMAA_ATTR_SHELL)},
      {-1},
#line 80 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str42, t(PBSDRMAA_ATTR_BULK_REQ)},
#line 68 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str43, t(PBSDRMAA_ATTR_HOST)},
#line 71 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str44, t(PBSDRMAA_ATTR_NCPUS)},
#line 56 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str45, t(PBSDRMAA_ATTR_KEEP_FILES)},
      {-1},
#line 72 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str47, t(PBSDRMAA_ATTR_SOFTWARE)},
#line 60 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str48, t(PBSDRMAA_ATTR_NICE)},
#line 69 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str49, t(PBSDRMAA_ATTR_NODES)},
#line 107 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str50, t(PBSDRMAA_ATTR_CUSTOM_RESOURCES)},
      {-1},
#line 66 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str52, t(PBSDRMAA_ATTR_WALLTIME_LIMIT)},
#line 86 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str53, t(PBSDRMAA_ATTR_JOB_NAME)},
#line 95 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str54, t(PBSDRMAA_ATTR_JOB_OWNER)},
#line 51 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str55, t(PBSDRMAA_ATTR_CHECKPOINT)},
      {-1}, {-1},
#line 59 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str58, t(PBSDRMAA_ATTR_FILE_SIZE_LIMIT)},
#line 106 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str59, t(PBSDRMAA_ATTR_NODE_PROPERTIES)},
#line 52 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str60, t(PBSDRMAA_ATTR_ERROR_PATH)},
      {-1}, {-1}, {-1},
#line 50 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str64, t(PBSDRMAA_ATTR_EXECUTION_TIME)},
#line 54 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str65, t(PBSDRMAA_ATTR_HOLD_TYPES)},
      {-1}, {-1}, {-1},
#line 55 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str69, t(PBSDRMAA_ATTR_JOIN_FILES)},
      {-1}, {-1}, {-1},
#line 61 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str73, t(PBSDRMAA_ATTR_VMEM_LIMIT)},
#line 65 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str74, t(PBSDRMAA_ATTR_SINGLE_VMEM_LIMIT)},
      {-1},
#line 101 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str76, t(PBSDRMAA_ATTR_EXIT_STATUS)},
      {-1}, {-1},
#line 73 "pbs_attrib_pro.gperf"
      {(int)(long)&((struct stringpool_t *)0)->stringpool_str79, t(PBSDRMAA_ATTR_PLACE)}
    };

  if (len <= MAX_WORD_LENGTH && len >= MIN_WORD_LENGTH)
    {
      register int key = hash (str, len);

      if (key <= MAX_HASH_VALUE && key >= 0)
        {
          register int o = wordlist[key].name;
          if (o >= 0)
            {
              register const char *s = o + stringpool;

              if (*str == *s && !strcmp (str + 1, s + 1))
                return &wordlist[key];
            }
        }
    }
  return 0;
}
#line 108 "pbs_attrib_pro.gperf"

#undef t

int
pbsdrmaa_pbs_attrib_by_name( const char *name )
{
	const struct pbs_attrib *found;
	found = pbsdrmaa_pbs_attrib_lookup( name, strlen(name) );
	if( found )
		return found->attr->code;
	else
		return -1;
}

static const fsd_attribute_t *
pbsdrmaa_pbs_template_by_name( const fsd_template_t *self, const char *name )
{
	const struct pbs_attrib *found;
	found = pbsdrmaa_pbs_attrib_lookup( name, strlen(name) );
	if( found != NULL )
	 {
		fsd_assert( found->attr - pbsdrmaa_pbs_attributes == found->attr->code );
		return found->attr;
	 }
	else
		fsd_exc_raise_fmt(
				FSD_ERRNO_INVALID_ARGUMENT,
				"invalid PBS attribute name: %s", name
				);
}


static const fsd_attribute_t *
pbsdrmaa_pbs_template_by_code( const fsd_template_t *self, int code )
{
	if( 0 <= code  &&  code < PBSDRMAA_N_PBS_ATTRIBUTES )
	 {
		fsd_assert( pbsdrmaa_pbs_attributes[code].code == code );
		return & pbsdrmaa_pbs_attributes[ code ];
	 }
	else
		fsd_exc_raise_fmt(
				FSD_ERRNO_INVALID_ARGUMENT,
				"invalid PBS attribute code: %d", code
				);
}


fsd_template_t *
pbsdrmaa_pbs_template_new(void)
{
	return fsd_template_new(
			pbsdrmaa_pbs_template_by_name,
			pbsdrmaa_pbs_template_by_code,
			PBSDRMAA_N_PBS_ATTRIBUTES
			);
}

const fsd_attribute_t pbsdrmaa_pbs_attributes[ PBSDRMAA_N_PBS_ATTRIBUTES ] = {
	{ "Execution_Time", PBSDRMAA_ATTR_EXECUTION_TIME, false },
	{ "Checkpoint", PBSDRMAA_ATTR_CHECKPOINT, false },
	{ "Error_Path", PBSDRMAA_ATTR_ERROR_PATH, false },
	{ "group_list", PBSDRMAA_ATTR_GROUPS, false },
	{ "Hold_Types", PBSDRMAA_ATTR_HOLD_TYPES, false },
	{ "Join_Path", PBSDRMAA_ATTR_JOIN_FILES, false },
	{ "Keep_Files", PBSDRMAA_ATTR_KEEP_FILES, false },
	{ "Resource_List.cput", PBSDRMAA_ATTR_CPU_TIME_LIMIT, false },
	{ "Resource_List.file", PBSDRMAA_ATTR_FILE_SIZE_LIMIT, false },
	{ "Resource_List.nice", PBSDRMAA_ATTR_NICE, false },
	{ "Resource_List.vmem", PBSDRMAA_ATTR_VMEM_LIMIT, false },
	{ "Resource_List.mem", PBSDRMAA_ATTR_MEM_LIMIT, false },
	{ "Resource_List.pcput", PBSDRMAA_ATTR_SINGLE_CPU_TIME_LIMIT, false },
	{ "Resource_List.pmem", PBSDRMAA_ATTR_SINGLE_RSS_LIMIT, false },
	{ "Resource_List.pvmem", PBSDRMAA_ATTR_SINGLE_VMEM_LIMIT, false },
	{ "Resource_List.walltime", PBSDRMAA_ATTR_WALLTIME_LIMIT, false },
	{ "Resource_List.arch", PBSDRMAA_ATTR_ARCHITECTURE, false },
	{ "Resource_List.host", PBSDRMAA_ATTR_HOST, false },
	{ "Resource_List.nodes", PBSDRMAA_ATTR_NODES, false },
	{ "Resource_List.procs", PBSDRMAA_ATTR_PROCS, false },
	{ "Resource_List.ncpus", PBSDRMAA_ATTR_NCPUS, false },
	{ "Resource_List.software", PBSDRMAA_ATTR_SOFTWARE, false },
	{ "Resource_List.place", PBSDRMAA_ATTR_PLACE, false },
	{ "Resource_List.select", PBSDRMAA_ATTR_SELECT, false },
	{ "Mail_Points", PBSDRMAA_ATTR_MAIL_POINTS, false },
	{ "Output_Path", PBSDRMAA_ATTR_OUTPUT_PATH, false },
	{ "Priority", PBSDRMAA_ATTR_PRIORITY, false },
	{ "destination", PBSDRMAA_ATTR_DESITINATION, false },
	{ "Rerunable", PBSDRMAA_ATTR_RERUNABLE, false },
	{ "job_array_request", PBSDRMAA_ATTR_BULK_REQ, false },
	{ "job_array_id", PBSDRMAA_ATTR_BULK_IDX, false },
	{ "User_List", PBSDRMAA_ATTR_USERS, false },
	{ "Variable_List", PBSDRMAA_ATTR_JOB_ENVIRONMENT, false },
	{ "Account_Name", PBSDRMAA_ATTR_ACCOUNT_NAME, false },
	{ "Mail_Users", PBSDRMAA_ATTR_EMAIL, false },
	{ "Job_Name", PBSDRMAA_ATTR_JOB_NAME, false },
	{ "Shell_Path_List", PBSDRMAA_ATTR_SHELL, false },
	{ "depend", PBSDRMAA_ATTR_DEPEND, false },
	{ "interactive", PBSDRMAA_ATTR_INTERACTIVE, false },
	{ "stagein", PBSDRMAA_ATTR_STAGEIN, false },
	{ "stageout", PBSDRMAA_ATTR_STAGEOUT, false },
	{ "exec_host", PBSDRMAA_ATTR_EXECUTION_HOST, false },
	{ "exec_vnode", PBSDRMAA_ATTR_EXECUTION_VNODE, false },
	{ "Job_Owner", PBSDRMAA_ATTR_JOB_OWNER, false },
	{ "resources_used", PBSDRMAA_ATTR_RESOURCES_USED, false },
	{ "job_state", PBSDRMAA_ATTR_JOB_STATE, false },
	{ "queue", PBSDRMAA_ATTR_QUEUE, false },
	{ "server", PBSDRMAA_ATTR_SERVER, false },
	{ "comment", PBSDRMAA_ATTR_COMMENT, false },
	{ "Exit_status", PBSDRMAA_ATTR_EXIT_STATUS, false },
	{ "start_time", PBSDRMAA_ATTR_START_TIME, false },
	{ "x", PBSDRMAA_ATTR_EXTENSION, false },
	{ "submit_args", PBSDRMAA_ATTR_SUBMIT_ARGS, false },
	{ "mtime", PBSDRMAA_ATTR_MTIME, false },
	{ "Resource_List.node_properties", PBSDRMAA_ATTR_NODE_PROPERTIES, false }, /* DRMAA extenstions */
	{ "Resource_List.custom_resources", PBSDRMAA_ATTR_CUSTOM_RESOURCES, false },
	
};

/* vim: set ft=c: */
