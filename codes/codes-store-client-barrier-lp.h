/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef CODES_STORE_CLIENT_BARRIER_LP_H
#define CODES_STORE_CLIENT_BARRIER_LP_H

/* this is an LP to simplify barrier workload processing for clients. All clients
 * directly contact this LP and, when all involved have checked in, this LP
 * acknowledges each of them */

#include <ross.h>
#include <codes/quicklist.h>

extern int barrier_magic;

void barrier_register();
void barrier_configure();

typedef struct barrier_msg barrier_msg;
typedef struct barrier_op barrier_op;

struct barrier_msg {
    int magic;
    // not needed - enum barrier_event event_type;

    /* sender ID info. Right now, the ranks are computed directly from the src
     * LP id)  */
    tw_lpid src;

    /* count and root of the caller barrier msg */
    int count;
    int root;
    int rank;

    int event_num;
};

struct barrier_op {
    int root, count;
    int checked_in_count;
    uint8_t *rank_stats; 
    struct qlist_head ql;
};
#endif /* end of include guard: CODES_STORE_CLIENT_BARRIER_LP_H */


/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */

