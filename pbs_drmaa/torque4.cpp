
#include "torque4.h"

void hash_add_or_exit_c(memmgr **mm, job_data **head, const char *name, const char *value, int var_type)
{
	hash_add_or_exit(mm, head, name, value, var_type);
}	

int memmgr_init_c(memmgr **mm, int size)
{
	return memmgr_init(mm,size);
}

void memmgr_destroy_c(memmgr **mm)
{
	memmgr_destroy(mm);
}
