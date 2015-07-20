/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include "wkld_config.h"
#include "codes/configuration.h"
#include "codes/codes-workload.h"
#include <assert.h>
#include <string.h>

void workload_configure(char ** wtype, void ** wparams, int num_clients){
    char tmp[MAX_NAME_LENGTH_WKLD];

    /* get the name of the workload */
    int rc = configuration_get_value(&config, "workload", "type",
            NULL, tmp, MAX_NAME_LENGTH_WKLD);
    assert(rc>0);

    *wtype = malloc(rc+1);
    strcpy(*wtype, tmp);

    int params_size, add_null = 0;
    void *params;
    union {
        struct iolang_params i_params;
        darshan_params d_params;
    } u;
    /* process the darshan workload */
    if (strcmp(*wtype, "darshan_io_workload") == 0){
        rc = configuration_get_value_relpath(&config, "workload", 
                "darshan_log_file", NULL, u.d_params.log_file_path, 
                MAX_NAME_LENGTH_WKLD);
        assert(rc > 0);
        int tmp;
        rc = configuration_get_value_int(&config, "workload", 
                "darshan_aggregator_count", NULL, &tmp);
        assert(rc == 0);
        u.d_params.aggregator_cnt = tmp;
        params_size = sizeof(u.d_params);
        params = &u.d_params;
    }
    /* process the iolang workload (the kernel language) */
    else if (strcmp(*wtype, "iolang_workload") == 0){
        rc = configuration_get_value_relpath(&config, "workload",
                "io_kernel_meta_path", NULL, u.i_params.io_kernel_meta_path,
                MAX_NAME_LENGTH_WKLD);
        assert(rc>0);
        u.i_params.use_relpath = 1; /* use relative paths */
        u.i_params.io_kernel_path[0]='\0';/* defined in meta file */
        u.i_params.num_cns = num_clients;
        params_size = sizeof(u.i_params);
        params = &u.i_params;
    }
    else {
        rc = configuration_get_value(&config, "workload", "params",
                NULL, tmp, MAX_NAME_LENGTH_WKLD);
        assert(rc>0);
        params_size = rc;
        params = tmp;
        add_null = 1;
    }

    *wparams = malloc(params_size+1);
    memcpy(*wparams, params, params_size);
    if (add_null) ((char*)*wparams)[params_size] = '\0';
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
