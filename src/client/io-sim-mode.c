/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <assert.h>

#include "io-sim-mode.h"

static char * mode_strings[4][7] = {
    {"uninitialized", "zero", "wkld-hash-id", "wkld-id",
        "random-persist", "placement"},
    {"uninitialized", "zero", "binned", "mod", "local"},
    {"uninitialized", "single", "round-robin"},
    {"uninitialized", "individual", "shared"}
};

void print_sim_modes(FILE *f, struct io_sim_config const * c){
    fprintf(f, "=== I/O Simulation Modes ===\n"
               "  oid mapping:        %s\n"
               "  placement:          %s\n"
               "  distribution:       %s\n"
               "  open:               %s\n",
               mode_strings[1][c->oid_gen_mode],
               mode_strings[2][c->placement_mode],
               mode_strings[3][c->dist_mode],
               mode_strings[4][c->open_mode]);
}

void io_sim_read_config(
        ConfigHandle *handle,
        char const * section_name,
        char const * annotation,
        int num_ranks,
        struct io_sim_config *cfg)
{
    char val[CONFIGURATION_MAX_NAME];
    int rc;

    cfg->req_cfg =
        codes_workload_read_config(handle, section_name, annotation,
                num_ranks);

    rc = configuration_get_value(handle, section_name, "oid_gen_mode",
            annotation, val, CONFIGURATION_MAX_NAME);
    assert(rc>0);
    if (strncmp(val, "zero", 4) == 0)
        cfg->oid_gen_mode = GEN_MODE_ZERO;
    else if (strncmp(val, "random_persist", 12) == 0)
        cfg->oid_gen_mode = GEN_MODE_RANDOM_PERSIST;
    else if (strncmp(val, "file_id_hash", 12) == 0)
        cfg->oid_gen_mode = GEN_MODE_WKLD_HASH;
    else if (strncmp(val, "file_id", 7) == 0)
        cfg->oid_gen_mode = GEN_MODE_WKLD_FILE_ID;
    else if (strncmp(val, "placement_random", 16) == 0)
        cfg->oid_gen_mode = GEN_MODE_PLACEMENT_RANDOM;
    else if (strncmp(val, "placement", 9) == 0)
        cfg->oid_gen_mode = GEN_MODE_PLACEMENT;
    else {
        fprintf(stderr, "unknown mode for %s:oid_gen_mode config entry\n",
                section_name);
        abort();
    }

    /* read placement mode */
    rc = configuration_get_value(handle, section_name, "placement_mode", NULL,
            val, CONFIGURATION_MAX_NAME);
    assert(rc>0);
    if (strncmp(val, "zero", 4) == 0)
        cfg->placement_mode = PLC_MODE_ZERO;
    else if (strncmp(val, "binned", 6) == 0)
        cfg->placement_mode = PLC_MODE_BINNED;
    else if (strncmp(val, "mod", 3) == 0)
        cfg->placement_mode = PLC_MODE_MOD;
    else if (strncmp(val, "local", 5) == 0)
        cfg->placement_mode = PLC_MODE_LOCAL;
    else {
        fprintf(stderr, "unknown mode for %s:placement_mode config entry\n",
                section_name);
        abort();
    }

    /* read striping mode */
    rc = configuration_get_value(handle, section_name, "dist_mode", NULL,
            val, CONFIGURATION_MAX_NAME);
    assert(rc>0);
    if (strncmp(val, "single", 6) == 0){
        cfg->dist_mode = DIST_MODE_SINGLE;
        cfg->dist_cfg.stripe_factor = 1;
        cfg->dist_cfg.strip_size = 0;
    }
    else if (strncmp(val, "rr", 2) == 0){
        cfg->dist_mode = DIST_MODE_RR;
        /* read additional parameters currently colocated with the triton
         * client */
        rc = configuration_get_value_int(handle, section_name,
                "stripe_factor", NULL, &cfg->dist_cfg.stripe_factor);
        assert(rc==0);
        rc = configuration_get_value_int(handle, section_name,
                "strip_size", NULL, &cfg->dist_cfg.strip_size);
        assert(rc==0);
    }
    else{
        fprintf(stderr, "unknown mode for %s:stripe_mode config entry\n",
                section_name);
        abort();
    }

    /* read file open mode (don't care about value, existence -> yes)
     * optional - absence -> no */
    int is_shared_open_mode = 0;
    configuration_get_value_int(&config, section_name,
            "shared_object_mode", annotation, &is_shared_open_mode);
    if (is_shared_open_mode)
        cfg->open_mode = OPEN_MODE_SHARED;
    else
        cfg->open_mode = OPEN_MODE_INDIVIDUAL;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
