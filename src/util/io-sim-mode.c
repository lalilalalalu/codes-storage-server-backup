/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include "io-sim-mode.h"

/* defaults are uninitialized */
enum io_sim_cli_req_mode_t io_sim_cli_req_mode = REQ_MODE_UNINIT;
enum io_sim_cli_oid_map_mode_t io_sim_cli_oid_map_mode = MAP_MODE_UNINIT;
enum io_sim_cli_dist_mode_t io_sim_cli_dist_mode = DIST_MODE_UNINIT;

static char * mode_strings[3][5] = {
    {"uninitialized", "mocked", "workload"},
    {"uninitialized", "zero", "random", "wkld-id", "wkld-hash-id"},
    {"uninitialized", "single", "round-robin"}
};

void print_sim_modes(FILE *f){
    fprintf(f, "=== I/O Simulation Modes ===\n"
               "request generation: %s\n"
               "oid mapping:        %s\n"
               "distribution:       %s\n",
               mode_strings[0][io_sim_cli_req_mode],
               mode_strings[1][io_sim_cli_oid_map_mode],
               mode_strings[2][io_sim_cli_dist_mode]);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
