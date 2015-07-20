/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef ROSD_COMMON_H
#define ROSD_COMMON_H

#define MAX_REPLICATION 4

typedef struct op_status{
    // status of local disk operation
    int local_status; 
    // status of forwarding operation (fanout requires tracking all at once)
    int fwd_status_ct;
    char fwd_status[MAX_REPLICATION];
} op_status;

#endif

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
