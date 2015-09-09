/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef IO_SIM_MODE_H
#define IO_SIM_MODE_H

#include <stdio.h>
#include <codes/configuration.h>
#include <codes/codes-workload.h>

/* There are numerous ways in which to run the client/server simulation. This
 * header and the related c file define some utilities to set/determine how the
 * simulation will run. Note that the respective variables (io_sim_*) MUST be
 * set prior to calling the LP initializers */

enum io_sim_cli_oid_gen_mode_t{
    /* uninitialized */
    GEN_MODE_UNINIT, 
    /* OID=0. Debug mode. */
    GEN_MODE_ZERO,
    /* use jenkins hash on the workload file id */
    GEN_MODE_WKLD_HASH,
    /* map workload file ids directly to oids, use placement to map to server.
     * Incompatible with non-workload request types */ 
    GEN_MODE_WKLD_FILE_ID,
    /* use random() call at open time to map file id into set of oids.
     * REQUIRES same srandom across all processes, no nondeterministic calls to
     * random in between opens */
    GEN_MODE_RANDOM_PERSIST,
    /* rely on the placement method being used to generate OIDs for us,
     * always starting from server 0 */
    GEN_MODE_PLACEMENT,
    /* instruct the placement algo to generate random OIDs and choose a
     * random start server (if doing LOCAL placement, then will just choose an
     * appropriate random OID. */
    GEN_MODE_PLACEMENT_RANDOM
};

enum io_sim_cli_placement_mode_t {
    // uninitialized
    PLC_MODE_UNINIT,
    // see oid_map types
    PLC_MODE_ZERO,
    PLC_MODE_BINNED,
    PLC_MODE_MOD,
    // map to a group-local codes-store, erroring out if unable to.
    // Uses OID_MAP_MOD under the covers
    // incompatible with striping
    PLC_MODE_LOCAL
};

enum io_sim_cli_dist_mode_t{
    /* uninitialized */
    DIST_MODE_UNINIT,
    /* distribute on a single OID */
    DIST_MODE_SINGLE,
    /* distribute using round-robin striping distribution. Requires config-time
     * params stripe_factor and strip_size. Incompatible with any
     * generation/placement modes that fix the OID or server */
    DIST_MODE_RR
};

enum io_sim_cli_open_mode_t {
    OPEN_MODE_UNINIT,
    // each client performs an open on the respective oid
    OPEN_MODE_INDIVIDUAL,
    // client 0 opens, and every client runs a barrier. incompatible with local
    // placement
    OPEN_MODE_SHARED
};

// configuration data structures for io sim modes

struct io_sim_cli_oid_gen_cfg { };
struct io_sim_cli_placement_cfg { };

struct io_sim_cli_dist_cfg {
    int stripe_factor;
    int strip_size;
};

struct io_sim_config {
    enum io_sim_cli_oid_gen_mode_t   oid_gen_mode;
    enum io_sim_cli_placement_mode_t placement_mode;
    enum io_sim_cli_dist_mode_t      dist_mode;
    enum io_sim_cli_open_mode_t      open_mode;

    codes_workload_config_return     req_cfg;
    struct io_sim_cli_oid_gen_cfg    oid_gen_cfg;
    struct io_sim_cli_placement_cfg  placement_cfg;
    struct io_sim_cli_dist_cfg       dist_cfg;
};

/* helpers */
void print_sim_modes(FILE *f, struct io_sim_config const * c);

void io_sim_read_config(
        ConfigHandle *handle,
        char const * section_name,
        char const * annotation,
        int num_ranks,
        struct io_sim_config *cfg);

/* an uninitialized state is an error state */
static inline int is_sim_mode_init(struct io_sim_config const * c){
    return c->oid_gen_mode   != GEN_MODE_UNINIT &&
           c->placement_mode != PLC_MODE_UNINIT &&
           c->dist_mode      != DIST_MODE_UNINIT &&
           c->open_mode      != OPEN_MODE_UNINIT;
}

static inline int is_striping_compatible(struct io_sim_config const * c){
    return c->oid_gen_mode   != GEN_MODE_ZERO &&
           c->placement_mode != PLC_MODE_ZERO &&
           c->placement_mode != PLC_MODE_LOCAL;
}

static inline int is_valid_sim_config(struct io_sim_config const * c){
    /* sim will break under certain combinations of behaviors. check them
     * here */
    return
        !(c->dist_mode == DIST_MODE_RR && !is_striping_compatible(c)) &&
        !(c->open_mode == OPEN_MODE_SHARED &&
                c->placement_mode == PLC_MODE_LOCAL);
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
