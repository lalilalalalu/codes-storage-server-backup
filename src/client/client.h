/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef CLIENT_H
#define CLIENT_H

#include <ross.h>
#include <codes/lp-msg.h>
#include <codes/codes-workload.h>
#include <codes/codes-store-lp.h>

extern int triton_client_magic;

extern char const * const CLIENT_LP_NM;

/* rosd needs to know client msg types, so expose here */ 
typedef struct triton_client_state triton_client_state;
typedef struct triton_client_msg triton_client_msg;

/* event types */
enum triton_client_event
{
    TRITON_CLI_KICKOFF = 10,
    TRITON_CLI_RECV_ACK,
    TRITON_CLI_WKLD_CONTINUE,
};

struct triton_client_msg {
    msg_header header;
    int tag;
    codes_store_ret_t ret;

    /* previous operation index for reverse computation */
    int op_index_prev;

    /* cache for codes workload operations - we need to provide it for 
     * workload reverse computation */
    struct codes_workload_op op;

    /* to prevent FP roundoff errors, stash previous full time here */
    double prev_time;

    int event_num;
};

void triton_client_init(
        int client_count,
        int server_count,
        char * wkld_type,
        char * wkld_params);

/* registers the lp type with ross */
void triton_client_register();
/* configures the lp given the global config object */
void triton_client_configure();

#endif /* end of include guard: CLIENT_H */


/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */

