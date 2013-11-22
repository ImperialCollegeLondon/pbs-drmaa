#ifdef __cplusplus

#include "u_hash_map_structs.h"

extern "C" {
#endif

typedef struct job_info
{
  memmgr *mm;
  job_data *job_attr;
  job_data *res_attr;
  job_data *user_attr;
  job_data *client_attr;
} job_info;


void hash_add_or_exit_c(memmgr **mm, job_data **head, const char *name, const char *value, int var_type);
int memmgr_init_c(memmgr **mm, int size);
void memmgr_destroy_c(memmgr **mm);

#ifdef __cplusplus
}
#endif
