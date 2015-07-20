/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <ross.h>
#include "map_util.h"
#include "codes/codes_mapping.h"


// TODO: be annotation-aware

/* rosd LPs */

/* NOTE: very inefficient, but sufficient for testing purposes */
tw_lpid get_rosd_lpid(int svr_id){
    // currently getting lp-id across all groups, annotations
    return codes_mapping_get_lpid_from_relative(svr_id, NULL, ROSD_LP_NM,
            NULL, 0);
}

/* NOTE: very inefficient */
int get_rosd_index(tw_lpid gid){
    return codes_mapping_get_lp_relative_id(gid, 0, 0);
}

/* client LPs */
tw_lpid get_client_lpid(int client_idx){
    return codes_mapping_get_lpid_from_relative(client_idx, NULL, CLIENT_LP_NM,
            NULL, 0);
}

int get_client_index(tw_lpid gid){
    return codes_mapping_get_lp_relative_id(gid, 0, 0);
}

static int is_barrier_init = 0;
static tw_lpid barrier_lpid;

tw_lpid get_barrier_lpid(){
    if (!is_barrier_init){
        is_barrier_init = 1;
        codes_mapping_get_lp_id("SINGLETON_GRP", "barrier", NULL, 1, 0, 0,
                &barrier_lpid);
    }
    return barrier_lpid;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
