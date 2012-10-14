#include <stdio.h>

#include <drmaa2.h>

int main (int argc, char **argv)
{
	drmaa2_jsession job_session = NULL;

	if ((job_session = drmaa2_create_jsession("my_session", NULL)) == NULL) {
		drmaa2_string errmsg = drmaa2_lasterror_text();
		drmaa2_error errcode = drmaa2_lasterror();
		fprintf (stderr, "Could not create Job Session: %s (errcode=%d)\n", errmsg, errcode);
		drmaa2_string_free(&errmsg);

		return 1;
	}

	printf("DRMAA 2.0 Job Session created successfully\n");

	if ((drmaa2_destroy_jsession("my_session") != DRMAA2_SUCCESS)) {
			drmaa2_string errmsg = drmaa2_lasterror_text();
			drmaa2_error errcode = drmaa2_lasterror();
			fprintf (stderr, "Could not destroy Job Session: %s (errcode=%d)\n", errmsg, errcode);
			drmaa2_string_free(&errmsg);

			drmaa2_jsession_free(&job_session);

			return 1;
		}

	drmaa2_jsession_free(&job_session);

	return 0;
}
