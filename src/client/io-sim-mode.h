/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef IO_SIM_MODE_H
#define IO_SIM_MODE_H

#include <stdio.h>

/* There are numerous ways in which to run the client/server simulation. This
 * header and the related c file define some utilities to set/determine how the
 * simulation will run. Note that the respective variables (io_sim_*) MUST be
 * set prior to calling the LP initializers */

enum io_sim_cli_req_mode_t{
    /* uninitialized */
    REQ_MODE_UNINIT,
    /* generate arbitrary requests from config file */
    REQ_MODE_MOCK,
    /* read and execute workloads from config file */
    REQ_MODE_WKLD,
};

enum io_sim_cli_oid_map_mode_t{
    /* uninitialized */
    MAP_MODE_UNINIT, 
    /* OID=0. Debug mode. */
    MAP_MODE_ZERO,
    /* use ROSS LP rng to generate oid for req, use placement to map to 
     * server. Incompatible with REQ_MODE_WKLD */
    MAP_MODE_RANDOM, 
    /* map workload file ids directly to oids, use placement to map to server.
     * Incompatible with non-workload request types */ 
    MAP_MODE_WKLD_FILE_ID,
    /* use jenkins hash on the workload file id */
    MAP_MODE_WKLD_HASH,
    /* use random() call at open time to map file id into set of oids. 
     * REQUIRES same srandom across all processes, no nondeterministic calls to
     * random in between opens */
    MAP_MODE_RANDOM_PERSIST,
};

enum io_sim_cli_dist_mode_t{
    /* uninitialized */
    DIST_MODE_UNINIT,
    /* distribute on a single OID */
    DIST_MODE_SINGLE,
    /* distribute using round-robin striping distribution. Requires config-time
     * params stripe_factor and strip_size. Incompatible with any mapping modes
     * that fix the OID */
    DIST_MODE_RR
};

/* sim run modes - must be initialized prior to tw_run() and LP init functions! */
extern enum io_sim_cli_req_mode_t io_sim_cli_req_mode;
extern enum io_sim_cli_oid_map_mode_t io_sim_cli_oid_map_mode;
extern enum io_sim_cli_dist_mode_t io_sim_cli_dist_mode;

/* helpers */
void print_sim_modes(FILE *f);

/* an uninitialized state is an error state */ 
static inline int is_sim_mode_init(){
    return io_sim_cli_req_mode     != REQ_MODE_UNINIT &&
           io_sim_cli_oid_map_mode != MAP_MODE_UNINIT && 
           io_sim_cli_dist_mode    != DIST_MODE_UNINIT;
}

static inline int is_valid_sim_config(){
    /* sim will break under certain combinations of behaviors. check them 
     * here */ 
    return 
        /* file-id mapping requires there to be a workload in the first place */
        !((io_sim_cli_req_mode      != REQ_MODE_WKLD && 
           (io_sim_cli_oid_map_mode  == MAP_MODE_WKLD_FILE_ID || 
            io_sim_cli_oid_map_mode  == MAP_MODE_WKLD_HASH ||
            io_sim_cli_oid_map_mode  == MAP_MODE_RANDOM_PERSIST)) || 
        /* if we're using workloads, then need a valid oid mapping */
          (io_sim_cli_req_mode     == REQ_MODE_WKLD && 
           io_sim_cli_oid_map_mode == MAP_MODE_RANDOM));
}

#endif

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
