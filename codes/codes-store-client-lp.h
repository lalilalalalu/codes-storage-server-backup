/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef CODES_STORE_CLIENT_LP_H
#define CODES_STORE_CLIENT_LP_H

#include <ross.h>
#include <codes/lp-msg.h>
#include <codes/codes-workload.h>
#include <codes/codes-store-lp.h>

extern int cs_client_magic;

extern char const * const CLIENT_LP_NM;

/* rosd needs to know client msg types, so expose here */ 
typedef struct cs_client_state cs_client_state;
typedef struct cs_client_msg cs_client_msg;

/* event types */
enum cs_client_event
{
    CS_CLI_KICKOFF = 10,
    CS_CLI_RECV_ACK,
    CS_CLI_WKLD_CONTINUE,
};

struct cs_client_msg {
    msg_header header;
    int tag;
    codes_store_ret_t ret;

    /* previous operation index for reverse computation */
    int op_index_prev;

    /* number of rng calls made to oid_map_create_stiped_random */
    int num_rng_calls;

    /* cache for codes workload operations - we need to provide it for 
     * workload reverse computation */
    struct codes_workload_op op;

    /* to prevent FP roundoff errors, stash previous full time here */
    double prev_time;

    int event_num;
};

/* registers the lp type with ross */
void cs_client_register();
/* configures the lp given the global config object */
void cs_client_configure(int model_net_id);

#endif /* end of include guard: CODES_STORE_CLIENT_LP_H */


/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */

