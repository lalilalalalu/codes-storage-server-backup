/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <codes/codes-store-client-barrier-lp.h>
#include <codes/codes-store-client-lp.h>
#include <codes/codes_mapping.h>
#include <codes/lp-type-lookup.h>
#include <codes/jenkins-hash.h>
#include <codes/codes.h>
#include <codes/quicklist.h>
#include <codes/lp-io.h>

#define BARRIER_DEBUG 0

/**** BEGIN SIMULATION DATA STRUCTURES ****/

int barrier_magic; /* use this as sanity check on events */

typedef struct barrier_state barrier_state;

struct barrier_state {
    struct qlist_head barrier_ops;
    int error_ct;
#if BARRIER_DEBUG
    int event_num;
    FILE *fdbg;
#endif
};

/**** END SIMULATION DATA STRUCTURES ****/

/**** BEGIN LP, EVENT PROCESSING FUNCTION DECLS ****/

/* ROSS LP processing functions */  
static void barrier_lp_init(
    barrier_state * ns,
    tw_lp * lp);
static void barrier_event_handler(
    barrier_state * ns,
    tw_bf * b,
    barrier_msg * m,
    tw_lp * lp);
static void barrier_rev_handler(
    barrier_state * ns,
    tw_bf * b,
    barrier_msg * m,
    tw_lp * lp);
static void barrier_finalize(
    barrier_state * ns,
    tw_lp * lp);

/* event type handlers */
static void handle_barrier(
    barrier_state * ns,
    barrier_msg * m,
    tw_lp * lp);
static void handle_barrier_rev(
    barrier_state * ns,
    barrier_msg * m,
    tw_lp * lp);

/* ROSS function pointer table for this LP */
tw_lptype barrier_lp = {
    (init_f) barrier_lp_init,
    (pre_run_f) NULL,
    (event_f) barrier_event_handler,
    (revent_f) barrier_rev_handler,
    (final_f)  barrier_finalize, 
    (map_f) codes_mapping,
    sizeof(barrier_state),
};

/**** END LP, EVENT PROCESSING FUNCTION DECLS ****/

/**** BEGIN IMPLEMENTATIONS ****/

void barrier_register(){
    lp_type_register("barrier", &barrier_lp);
}

void barrier_configure(){
    uint32_t h1=0, h2=0;

    bj_hashlittle2("barrier", strlen("barrier"), &h1, &h2);
    barrier_magic = h1+h2;
}

void barrier_lp_init(
        barrier_state * ns,
        tw_lp * lp){
    INIT_QLIST_HEAD(&ns->barrier_ops);
    ns->error_ct = 0;
#if BARRIER_DEBUG
    char val[64];
    sprintf(val, "barrier.%lu",lp->gid);
    ns->fdbg = fopen(val, "w");
    assert(ns->fdbg);
    setvbuf(ns->fdbg, NULL, _IONBF, 0);
#endif
}

void barrier_event_handler(
        barrier_state * ns,
        tw_bf * b,
        barrier_msg * m,
        tw_lp * lp){
    assert(m->magic == barrier_magic);
    
    if (ns->error_ct > 0){
        ns->error_ct++;
        return;
    }

    handle_barrier(ns,m,lp);
#if BARRIER_DEBUG
    ns->event_num++;
#endif
}

void barrier_rev_handler(
        barrier_state * ns,
        tw_bf * b,
        barrier_msg * m,
        tw_lp * lp){
    assert(m->magic == barrier_magic);
    
    if (ns->error_ct > 0){
        ns->error_ct--;
        if (ns->error_ct==0){
            lp_io_write_rev(lp->gid, "errors");
#if BARRIER_DEBUG
            fprintf(ns->fdbg, "left bad state through reverse\n");
#endif
        }
        return;
    }
    handle_barrier_rev(ns,m,lp);
}

void barrier_finalize(
        barrier_state * ns,
        tw_lp * lp){
    /* nothin */
}

/* event type handlers */
void handle_barrier(
        barrier_state * ns,
        barrier_msg * m,
        tw_lp * lp){
    struct qlist_head *ent;
    barrier_op *op;
#if BARRIER_DEBUG
    fprintf(ns->fdbg, "barrier: rank:%d, count:%d, root:%d, event_num:%d\n", m->rank, m->count, m->root, m->event_num);
#endif
    qlist_for_each(ent, &ns->barrier_ops){
        barrier_op *tmp = qlist_entry(ent,barrier_op,ql);
        if (tmp->root == m->root){
            op = tmp;
            break;
        }
    }
    if (ent == &ns->barrier_ops){
        /* new entry needs to be made */
        op = malloc(sizeof(barrier_op));
        op->rank_stats = calloc(m->count, sizeof(*op->rank_stats));
        op->count = m->count;
        op->root = m->root;
        op->checked_in_count = 0;
        qlist_add_tail(&op->ql, &ns->barrier_ops);
    }

    /* sanity checks */
    assert(m->rank >= op->root && m->rank < op->root + op->count);

    if (op->rank_stats[m->rank] == 1){
        char buf[128];
        int written = sprintf(buf, "barrier: too many barrier req from client\n");
        lp_io_write(lp->gid, "errors", written, buf);
        ns->error_ct = 1;
#if BARRIER_DEBUG
        fprintf(ns->fdbg, "entered bad state\n");
#endif
        return;
    }
    assert(op->rank_stats[m->rank] != 1);
    if (op->checked_in_count >= op->count){
        char buf[128];
        int written = sprintf(buf, "barrier: too many overall reqs\n");
        lp_io_write(lp->gid, "errors", written, buf);
        ns->error_ct = 1;
#if BARRIER_DEBUG
        fprintf(ns->fdbg, "entered bad state\n");
#endif
        return;
    }
    /*assert(op->checked_in_count < op->count);*/

    op->rank_stats[m->rank]++;
    assert(op->rank_stats[m->rank] < 2);
    op->checked_in_count++;
    
    if (op->checked_in_count == op->count){
        /* done, send all acks back */
        int i;
        for (i = 0; i < op->count; i++){
            tw_lpid cli_lp =
                codes_mapping_get_lpid_from_relative(op->root+i, NULL,
                        CLIENT_LP_NM, NULL, 0);
            tw_event *e = codes_event_new(cli_lp,
                    codes_local_latency(lp), lp);
            cs_client_msg *m_ack = tw_event_data(e);
            m_ack->header.magic = cs_client_magic;
            m_ack->header.event_type = CS_CLI_WKLD_CONTINUE;
            m_ack->header.src = lp->gid;
#if BARRIER_DEBUG
            fprintf(ns->fdbg, "barrier: acking to cli %d, lp %lu\n", op->root+i,
                    cli_lp);
            m_ack->event_num = ns->event_num;
#endif
            tw_event_send(e);
        }
        free(op->rank_stats);
        qlist_del(&op->ql);
    }
}
void handle_barrier_rev(
        barrier_state * ns,
        barrier_msg * m,
        tw_lp * lp){
    struct qlist_head *ent;
    barrier_op *op;
    qlist_for_each(ent, &ns->barrier_ops){
        barrier_op *tmp = qlist_entry(ent,barrier_op,ql);
        if (tmp->root == m->root){
            op = tmp;
            break;
        }
    }
#if BARRIER_DEBUG
    fprintf(ns->fdbg, "barrier: rank:%d, count:%d, root:%d, event_num:%d REV\n", m->rank, m->count, m->root, m->event_num);
#endif
    if (ent == &ns->barrier_ops){
        /* item was recently removed, re-create */
        op = malloc(sizeof(barrier_op));
        op->rank_stats = malloc(m->count * sizeof(*op->rank_stats));
        op->count = m->count;
        op->root  = m->root;
        op->checked_in_count = op->count-1;
        /* ROSS ordering ensures us that this event caused removal from
         * the queue */ 
        int i;
        for (i = 0; i < op->count; i++){
            codes_local_latency_reverse(lp);
            op->rank_stats[i] = 1;
        }
        op->rank_stats[m->rank-op->root] = 0;
        qlist_add_tail(&op->ql, &ns->barrier_ops);
    }
    else{
        /* undo individual addition */
        assert(op->checked_in_count > 0);
        op->checked_in_count--;
        op->rank_stats[m->rank-op->root]--;
        assert(op->rank_stats[m->rank-op->root] == 0);
        if (op->checked_in_count == 0){
            /* remove completely, this is the first barrier */
            free(op->rank_stats);
            qlist_del(&op->ql);
        }
    }
}

/**** END IMPLEMENTATIONS ****/

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
