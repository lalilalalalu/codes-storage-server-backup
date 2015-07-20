/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <string.h>
#include <assert.h>
#include "codes/model-net.h"
#include "codes/codes_mapping.h"
#include "codes/lp-type-lookup.h"
#include "codes/jenkins-hash.h"
#include "codes/codes.h"
#include "codes/lp-io.h"
#include "codes/local-storage-model.h"
#include "codes/model-net-sched.h"
#include "codes/rc-stack.h"
#include "rosd.h"
#include "rosd-creq.h"
#include "../client/client.h"
#include "../util/msg.h"
#include "../input-generator/placement.h"
#include "../util/io-sim-mode.h"
#include "../util/map_util.h"

/// danger: debug messages will produce a LOT of output, even for small runs
/// and especially in optimistic runs

// thread specific debug messages (producing a finer-grain log)
#define ROSD_THREAD_DBG 0
#if ROSD_THREAD_DBG
#define tprintf(_fmt, ...) printf(_fmt, __VA_ARGS__)
#else
#define tprintf(_fmt, ...)
#endif

// lp specific debug messages (producing a coarser-grain log)
#define ROSD_LP_DBG 0
#if ROSD_LP_DBG
#define lprintf(_fmt, ...) printf(_fmt, __VA_ARGS__)
#else
#define lprintf(_fmt, ...)
#endif

// print rng stats at end (messes up some results scripts)
#define ROSD_PRINT_RNG 0

int rosd_magic = 0;

static rosd_fwd_mode fwd_mode;
static rosd_ack_mode ack_mode;

/// forwarding protocol strings
#define X(a,b) b,
static char const * const fwd_mode_strs[] = { FWD_MODES };
static char const * const ack_mode_strs[] = { ACK_MODES };
#undef X

extern int model_net_id;

/* system parameters */
static int num_servers;
int replication_factor;
static int num_threads = 4;
static int pipeline_unit_size = (1<<22);

/* disable lp output unless told otherwise */
int do_rosd_lp_io = 0;
lp_io_handle rosd_io_handle;

///// BEGIN LP, EVENT PROCESSING FUNCTION DECLS /////

// ROSS LP processing functions
static void triton_rosd_init(triton_rosd_state *ns, tw_lp *lp);
static void triton_rosd_event_handler(
        triton_rosd_state * ns,
        tw_bf * b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void triton_rosd_event_handler_rc(
        triton_rosd_state * ns,
        tw_bf * b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void triton_rosd_finalize(triton_rosd_state *ns, tw_lp *lp);

// event handlers
static void handle_recv_io_req(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_pipeline_alloc_callback(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_recv_chunk(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_complete_disk_op(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_recv_metadata_ack(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_recv_metadata_fwd_ack(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_recv_data_ack(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_recv_chunk_fwd_ack(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_complete_chunk_send(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);

static void handle_recv_io_req_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_pipeline_alloc_callback_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_recv_chunk_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_complete_disk_op_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_recv_metadata_ack_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_recv_metadata_fwd_ack_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_recv_data_ack_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_recv_chunk_fwd_ack_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);
static void handle_complete_chunk_send_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp);

///// END LP, EVENT PROCESSING FUNCTION DECLS /////

///// BEGIN SIMULATION DATA STRUCTURES /////

tw_lptype triton_rosd_lp = {
    (init_f) triton_rosd_init,
    (pre_run_f) NULL,
    (event_f) triton_rosd_event_handler,
    (revent_f) triton_rosd_event_handler_rc,
    (final_f) triton_rosd_finalize,
    (map_f) codes_mapping,
    sizeof(triton_rosd_state),
};

///// END SIMULATION DATA STRUCTURES /////

static uint64_t minu64(uint64_t a, uint64_t b) { return a < b ? a : b; }

void rosd_register()
{
    lp_type_register(ROSD_LP_NM, &triton_rosd_lp);
}

void rosd_configure(){
    uint32_t h1=0, h2=0;

    bj_hashlittle2(ROSD_LP_NM, strlen(ROSD_LP_NM), &h1, &h2);
    rosd_magic = h1+h2;

    /* get the number of servers by querying codes-mapping */
    // TODO: be annotation-aware
    num_servers = codes_mapping_get_lp_count(NULL, 0, ROSD_LP_NM, NULL, 1);

    int rc = configuration_get_value_int(&config, ROSD_LP_NM, 
            "replication_factor", NULL, &replication_factor);
    assert(rc==0);
    assert(replication_factor <= MAX_REPLICATION);

    char val[MAX_NAME_LENGTH];
    /* get the forwarding mode */
    rc = configuration_get_value(&config, ROSD_LP_NM, "forward_mode", NULL,
            val, MAX_NAME_LENGTH);
    assert(rc>0);
    if (rosd_get_fwd_mode(val, &fwd_mode)){
        fprintf(stderr, "unknown mode for rosd:forward_mode config entry\n");
        exit(1);
    }
    /* get the acking mode */
    rc = configuration_get_value(&config, ROSD_LP_NM, "ack_mode", NULL,
            val, MAX_NAME_LENGTH);
    assert(rc>0);
    if (rosd_get_ack_mode(val, &ack_mode)){
        fprintf(stderr, "unknown mode for rosd:ack_mode config entry\n");
        exit(1);
    }

    // get the number of threads and the pipeline buffer size
    // if not available, no problem - use a default of 4 threads, 4MB per
    // thread
    rc = configuration_get_value_int(&config, ROSD_LP_NM, "req_threads", 
            NULL, &num_threads);
    rc = configuration_get_value_int(&config, ROSD_LP_NM, "thread_buf_sz",
            NULL, &pipeline_unit_size);

    /* check the modes for compatibility */
    if (!is_rosd_valid_fwd_config(fwd_mode, ack_mode)){
        fprintf(stderr, "fwd/ack modes are incompatible\n");
        exit(1);
    }

    /* get the placement algorithm */
    rc = configuration_get_value(&config, ROSD_LP_NM, "placement", 
            NULL, val, MAX_NAME_LENGTH);
    assert(rc>0);
    placement_set(val, (unsigned int)num_servers);

    /* done!!! */
}

///// BEGIN LP, EVENT PROCESSING FUNCTION DEFS /////


void triton_rosd_init(triton_rosd_state *ns, tw_lp *lp) {
    ns->server_index = get_rosd_index(lp->gid);
    assert(ns->server_index < num_servers);

    ns->oid_srv_map = malloc(replication_factor * sizeof(*ns->oid_srv_map));
    assert(ns->oid_srv_map);

    INIT_QLIST_HEAD(&ns->pending_pipeline_ops);
    rc_stack_create(&ns->finished_pipeline_ops);
    INIT_QLIST_HEAD(&ns->pending_chunk_ops);
    rc_stack_create(&ns->finished_chunk_ops);
    INIT_QLIST_HEAD(&ns->pending_meta_ops);
    rc_stack_create(&ns->finished_meta_ops);

    ns->op_idx_pl = 0;
}

void triton_rosd_event_handler(
        triton_rosd_state * ns,
        tw_bf * b,
        triton_rosd_msg * m,
        tw_lp * lp) {
    assert(m->h.magic == rosd_magic);

    /* check error_ct: only process events when not in "suspend" mode 
     * - NOTE: this impl counts all forward events since the error 
     *   condition was reached, only allowing progress when we are back to
     *   normal */
    if (ns->error_ct > 0){
        ns->error_ct++;
        return;
    }

    /* perform a garbage collection */
    rc_stack_gc(lp, 1, ns->finished_pipeline_ops);
    rc_stack_gc(lp, 1, ns->finished_chunk_ops);

    switch (m->h.event_type){
        case RECV_CLI_REQ:
            handle_recv_io_req(ns, b, m, lp);
            break;
        case PIPELINE_ALLOC_CALLBACK:
            handle_pipeline_alloc_callback(ns, b, m, lp);
            break;
        case RECV_CHUNK:
            handle_recv_chunk(ns, b, m, lp);
            break;
        case RECV_SRV_REQ:
            // uses same code path as client with hooks
            handle_recv_io_req(ns, b, m, lp);
            break;
        case COMPLETE_DISK_OP:
            handle_complete_disk_op(ns, b, m, lp);
            break;
        case RECV_METADATA_ACK:
            handle_recv_metadata_ack(ns, b, m, lp);
            break;
        case RECV_METADATA_FWD_ACK:
            handle_recv_metadata_fwd_ack(ns, b, m, lp);
            break;
        case RECV_DATA_ACK:
            handle_recv_data_ack(ns, b, m, lp);
            break;
        case RECV_CHUNK_FWD_ACK:
            handle_recv_chunk_fwd_ack(ns, b, m, lp);
            break;
        case COMPLETE_CHUNK_SEND:
            handle_complete_chunk_send(ns, b, m, lp);
            break;
        default:
            tw_error(TW_LOC, "unknown rosd event type");
    }
}

void triton_rosd_event_handler_rc(
        triton_rosd_state * ns,
        tw_bf * b,
        triton_rosd_msg * m,
        tw_lp * lp) {
    assert(m->h.magic == rosd_magic);
    
    /* check error_ct: only process events when not in "suspend" mode 
     * - NOTE: this impl counts all forward events since the error 
     *   condition was reached, only allowing progress when we are back to
     *   normal */
    if (ns->error_ct > 0){
        ns->error_ct--;
        if (ns->error_ct == 0){
            lp_io_write_rev(lp->gid, "errors");
        }
        return;
    }

    switch (m->h.event_type){
        case RECV_CLI_REQ:
            handle_recv_io_req_rc(ns, b, m, lp);
            break;
        case PIPELINE_ALLOC_CALLBACK:
            handle_pipeline_alloc_callback_rc(ns, b, m, lp);
            break;
        case RECV_CHUNK:
            handle_recv_chunk_rc(ns, b, m, lp);
            break;
        case RECV_SRV_REQ:
            handle_recv_io_req_rc(ns, b, m, lp);
            break;
        case COMPLETE_DISK_OP:
            handle_complete_disk_op_rc(ns, b, m, lp);
            break;
        case RECV_METADATA_ACK:
            handle_recv_metadata_ack_rc(ns, b, m, lp);
            break;
        case RECV_METADATA_FWD_ACK:
            handle_recv_metadata_fwd_ack_rc(ns, b, m, lp);
            break;
        case RECV_DATA_ACK:
            handle_recv_data_ack_rc(ns, b, m, lp);
            break;
        case RECV_CHUNK_FWD_ACK:
            handle_recv_chunk_fwd_ack_rc(ns, b, m, lp);
            break;
        case COMPLETE_CHUNK_SEND:
            handle_complete_chunk_send_rc(ns, b, m, lp);
            break;
        default:
            tw_error(TW_LOC, "unknown rosd event type");
    }

    // reset b: ROSS doesn't guarantee bitfield resetting in the case of
    // multiple rollbacks
    *(int*)b = 0;
}

void triton_rosd_finalize(triton_rosd_state *ns, tw_lp *lp) {
    free(ns->oid_srv_map);
    rc_stack_destroy(1, ns->finished_pipeline_ops);
    rc_stack_destroy(1, ns->finished_chunk_ops);

    // check for pending operations that did not complete or were not removed
    struct qlist_head *ent;
    qlist_for_each(ent, &ns->pending_pipeline_ops){
        rosd_pipeline_qitem *qi = qlist_entry(ent, rosd_pipeline_qitem, ql);
        fprintf(stderr, "WARNING: LP %lu with incomplete qitem "
                "(cli lp %lu, op id %d)\n",
                lp->gid, qi->cli_lp, qi->op_id);
    }
    qlist_for_each(ent, &ns->pending_chunk_ops){
        rosd_chunk_req_info *info = qlist_entry(ent, rosd_chunk_req_info, ql);
        fprintf(stderr, "WARNING: LP %lu with incomplete chunk op item "
            "(cli lp %lu, op id %d)\n",
            lp->gid, info->cli_src, info->cli_op_id);
    }
    int written = 0;
    if (ns->server_index == 0){
        written = sprintf(ns->output_buf, 
                "# Format: <server id> <LP id> bytes <read> <written> "
                "<returned (client rd)> <forwarded>"
#if ROSD_PRINT_RNG == 1
                " <rng model> <rng codes>"
#endif
                "\n");
    }
    written += sprintf(ns->output_buf+written,
            "%d %lu %lu %lu %lu %lu"
#if ROSD_PRINT_RNG == 1
            " %lu %lu"
#endif
            "\n",
            ns->server_index, lp->gid,
            ns->bytes_read_local, ns->bytes_written_local,
            ns->bytes_returned, ns->bytes_forwarded
#if ROSD_PRINT_RNG == 1
            , lp->rng[0].count, lp->rng[1].count
#endif
            );
    lp_io_write(lp->gid, "rosd-stats", written, ns->output_buf);
}

// bitfields used:
// c31 - alloc not called
//
// NOTE: assumes that there is an allocation to be performed
static void pipeline_alloc_event(
        tw_bf *b,
        tw_lp *lp,
        int op_id,
        rosd_pipelined_req *req){

    assert(req->rem > 0);
    assert(req->nthreads_init < req->nthreads);

    uint64_t sz = minu64(req->punit_size, req->rem);

    // init the thread, increment the waiting count
    int tid = req->nthreads_init;
    tprintf("%lu,%d: thread %d allocating, tid counts (%d+1, %d+1, %d)\n",
            lp->gid, op_id, tid, req->nthreads_init,
            req->nthreads_alloc_waiting, req->nthreads_fin);
    req->threads[tid].punit_size = sz;
    req->nthreads_alloc_waiting++;
    req->nthreads_init++;


    rosd_callback_id id;
    id.op_id = op_id;
    id.tid = tid;

    msg_header h;
    msg_set_header(rosd_magic, PIPELINE_ALLOC_CALLBACK, lp->gid, &h);

    // note - this is a "blocking" call - we won't get the callback until 
    // allocation succeeded
    resource_lp_get(&h, sz, 1, sizeof(triton_rosd_msg),
            offsetof(triton_rosd_msg, h), 
            offsetof(triton_rosd_msg, u.palloc_callback.cb),
            sizeof(rosd_callback_id), 
            offsetof(triton_rosd_msg, u.palloc_callback.id), &id, lp);
}

/// this is the handler for both client-sourced requests and server-sourced
/// - the logic is nearly the same, just need a few checks
/// perform initial object checks, begin a pipelined operation
// bitfields used:
// - c0 - chunk metadata was created
void handle_recv_io_req(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){

    // verify object status on server 
    // TODO: currently just checks against placement
    int is_obj_on_server = 0;
    placement_find_closest(m->u.creq.req.oid, replication_factor, 
            ns->oid_srv_map);
    for (int srv_map_pos = 0; srv_map_pos < replication_factor; 
            srv_map_pos++){
        if ((int)ns->oid_srv_map[srv_map_pos] == ns->server_index){
            is_obj_on_server = 1;
            break;
        }
    }
    if (!is_obj_on_server){
        tw_error(TW_LOC, 
                "object not resident on server, TODO: handle properly\n");
    }

    if (m->u.creq.req.req_type == REQ_OPEN) {
        tw_event *e_local;
        triton_rosd_msg *m_local;
        triton_rosd_msg m_fwd;
        rosd_callback_id cb_id;
        rosd_meta_qitem *qm = malloc(sizeof(*qm));
        assert(qm);

        qm->op_id = ns->op_idx_pl++;
        qm->src_lp = m->h.src;
        qm->status.local_status = 0;
        qm->req =
            (m->h.event_type==RECV_CLI_REQ) ? m->u.creq.req : m->u.sreq.req;
        cb_id.op_id = qm->op_id;
        cb_id.tid = -1;

        /* in both cases, send the local disk op */
        e_local = lsm_event_new("rosd", lp->gid, qm->req.oid, 0, 0,
                (qm->req.create) ? LSM_WRITE_REQUEST : LSM_READ_REQUEST,
                sizeof(*m_local), lp, 0.0);
        m_local = lsm_event_data(e_local);
        msg_set_header(rosd_magic, COMPLETE_DISK_OP, lp->gid, &m_local->h);
        m_local->u.complete_sto.id = cb_id;
        m_local->u.complete_sto.is_data_op = 0;
        tw_event_send(e_local);

        // we may not use it, but set the forwarding message up anyways
        msg_set_header(rosd_magic, RECV_SRV_REQ, lp->gid, &m_fwd.h);
        m_fwd.u.sreq.req = qm->req;
        m_fwd.u.sreq.total_req_sz = 0;
        m_fwd.u.sreq.id = cb_id;

        // go ahead and push the partially complete op
        qlist_add_tail(&qm->ql, &ns->pending_meta_ops);


        MN_START_SEQ();
        if (m->h.event_type == RECV_CLI_REQ) {
            m->u.creq.rc.op_id = qm->op_id;
            // if we're runnign primary recv, we can go ahead and ack the
            // client
            if (ack_mode == ACK_PRIMARY_RECV)
                triton_send_response(&m->u.creq.callback, &m->u.creq.req, lp,
                        model_net_id, ROSD_REQ_CONTROL_SZ, 0);
            qm->cli_lp = m->h.src;
            qm->chain_pos = 0;
            qm->cli_cb = m->u.creq.callback;
            qm->status.fwd_status_ct = 0;
            if (fwd_mode == FWD_FAN){
                memset(qm->status.fwd_status, 0,
                        replication_factor*sizeof(*qm->status.fwd_status));
                if (ack_mode == ACK_ALL_RECV) {
                    qm->recv_status_ct = 0;
                    memset(qm->recv_status, 0,
                            replication_factor*sizeof(*qm->recv_status));
                }
            }
            /* send out the forwarding calls */
            if (replication_factor > 1 && qm->req.create) {
                int num_fwds;
                int i;
                m_fwd.u.sreq.callback = m->u.creq.callback;

                num_fwds = (fwd_mode == FWD_FAN) ? replication_factor-1 : 1;
                for (i = 0; i < num_fwds; i++) {
                    tw_lpid dst = get_rosd_lpid((int)ns->oid_srv_map[i+1]);
                    int prio = 0;
                    model_net_set_msg_param(MN_MSG_PARAM_SCHED,
                            MN_SCHED_PARAM_PRIO, (void*) &prio);
                    // for simplicity, set chain pos to position in oid_srv_map
                    m_fwd.u.sreq.chain_pos = i+1;
                    model_net_event(model_net_id, "rosd", dst,
                            ROSD_REQ_CONTROL_SZ, 0.0, sizeof(m_fwd), &m_fwd, 0,
                            NULL, lp);
                }
            }
        }
        else {
            m->u.sreq.rc.op_id = qm->op_id;
            qm->cli_lp = m->u.sreq.callback.header.src;
            qm->chain_pos = m->u.sreq.chain_pos;
            qm->cli_cb = m->u.sreq.callback;
            qm->rosd_cb = m->u.sreq.id;
            qm->status.fwd_status_ct = 0;
            // send off the next forward if needed
            if (fwd_mode == FWD_CHAIN) {
                if (qm->chain_pos < replication_factor-1 && qm->req.create) {
                    int prio = 0;
                    tw_lpid dst =
                        get_rosd_lpid((int)ns->oid_srv_map[qm->chain_pos+1]);
                    m_fwd.u.sreq.callback = m->u.sreq.callback;
                    m_fwd.u.sreq.chain_pos = qm->chain_pos + 1;
                    model_net_set_msg_param(MN_MSG_PARAM_SCHED,
                            MN_SCHED_PARAM_PRIO, (void*) &prio);
                    model_net_event(model_net_id, "rosd", dst,
                            ROSD_REQ_CONTROL_SZ, 0.0, sizeof(m_fwd), &m_fwd, 0,
                            NULL, lp);
                }
                if (qm->chain_pos == replication_factor-1 &&
                        ack_mode == ACK_ALL_RECV)
                    triton_send_response(&m->u.sreq.callback, &m->u.sreq.req, lp,
                            model_net_id, ROSD_REQ_CONTROL_SZ, 0);
            }
            // send an internal ack if running fan+all_recv
            if (fwd_mode == FWD_FAN && ack_mode == ACK_ALL_RECV) {
                triton_rosd_msg m_ack;
                int prio = 0;
                msg_set_header(rosd_magic, RECV_METADATA_ACK,
                        lp->gid, &m_ack.h);
                m_ack.u.recv_meta_ack.op_id = m->u.sreq.id.op_id;
                m_ack.u.recv_meta_ack.chain_pos = qm->chain_pos;
                model_net_set_msg_param(MN_MSG_PARAM_SCHED,
                        MN_SCHED_PARAM_PRIO, (void*) &prio);
                model_net_event(model_net_id, "rosd", qm->src_lp,
                        ROSD_REQ_CONTROL_SZ, 0.0, sizeof(m_ack), &m_ack, 0,
                        NULL, lp);
            }
        }

        MN_END_SEQ();
        return;
    }
#if 0
    if (m->h.event_type == RECV_CLI_REQ && m->u.creq.req.req_type == REQ_OPEN){
        triton_send_response(&m->u.creq.callback, &m->u.creq.req, lp,
                model_net_id, ROSD_REQ_CONTROL_SZ, 0);
        return;
    }
    else{
        // TODO: currently don't handle non-primary opens 
        assert(!(m->h.event_type == RECV_SRV_REQ && 
                 m->u.sreq.req.req_type == REQ_OPEN));
    }
#endif

    // initialize a pipelining operation
    rosd_pipeline_qitem *qi = malloc(sizeof(rosd_pipeline_qitem)); 
    qi->op_id = ns->op_idx_pl++;
    qi->src_lp = m->h.src;

    if (m->h.event_type == RECV_CLI_REQ) {
        // save op id for rc
        m->u.creq.rc.op_id = qi->op_id;
        // client->server request - full pipeline
        qi->cli_lp = m->h.src;
        qi->req = rosd_pipeline_init(num_threads, pipeline_unit_size,
                &m->u.creq.req, &m->u.creq.callback);
        qi->chain_pos = 0;
        qi->cli_cb = m->u.creq.callback;
        qi->type = m->u.creq.req.req_type;
        // if we're in FAN+ALL_RECV, then we use a special status to track from
        // all in the fan
        if (fwd_mode == FWD_FAN && ack_mode == ACK_ALL_RECV){
            memset(qi->recv_status, 0, 
                    replication_factor*sizeof(*qi->recv_status));
            qi->recv_status_ct = 0;
        }
    }
    else{
        // in the server-server case, we're using a one thread 'pipeline' to
        // use the same code path
        m->u.sreq.rc.op_id = qi->op_id;
        qi->cli_lp = m->u.sreq.callback.header.src;
        qi->req = rosd_pipeline_init(1, pipeline_unit_size,
                &m->u.sreq.req, &m->u.sreq.callback);
        qi->chain_pos = m->u.sreq.chain_pos;
        qi->cli_cb = m->u.sreq.callback;
        qi->rosd_cb = m->u.sreq.id;
        qi->type = m->u.sreq.req.req_type;
        // in addition, search for or init the chunk request
        // NOTE: use the *client's* lp and op id as the identifier - op id's in 
        // non primaries are not request specific
        struct qlist_head *ent;
        rosd_chunk_req_info *id;
        qlist_for_each(ent, &ns->pending_chunk_ops){
            id = qlist_entry(ent, rosd_chunk_req_info, ql);
            if (id->cli_src == qi->cli_lp &&
                    id->cli_op_id == qi->cli_cb.op_index){
                break;
            }
        }
        if (ent == &ns->pending_chunk_ops){
            id = malloc(sizeof(rosd_chunk_req_info));
            id->cli_src = qi->cli_lp;
            id->cli_op_id = qi->cli_cb.op_index;
            id->total = m->u.sreq.total_req_sz;
            id->received = 0;
            qlist_add_tail(&id->ql, &ns->pending_chunk_ops);
            b->c0 = 1;
        }
    }
    lprintf("%lu: new req id:%d from %lu\n", lp->gid, qi->op_id, qi->src_lp);
    qlist_add_tail(&qi->ql, &ns->pending_pipeline_ops);

    // send the initial allocation event 
    pipeline_alloc_event(b, lp, qi->op_id, qi->req);
}

// bitfields used:
// c0 - there was data to pull post- thread allocation
// c1 - assuming  c0, thread is last to alloc due to no remaining data after
// c2 - assuming  c0 and !c1, thread was not the last thread and issued an alloc
// c3 - assuming !c0, thread is the last active, so pipeline request was deleted
//      (and moved to the finished queue)
void handle_pipeline_alloc_callback(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){

    // getting a failed allocation at this point is a fatal error since we are
    // using the blocking version of buffer acquisition
    assert(!m->u.palloc_callback.cb.ret);

    // find the corresponding operation
    struct qlist_head *ent = NULL;
    rosd_pipeline_qitem *qi = NULL; 
    qlist_for_each(ent, &ns->pending_pipeline_ops){
        qi = qlist_entry(ent, rosd_pipeline_qitem, ql);
        if (qi->op_id == m->u.palloc_callback.id.op_id){
            break;
        }
    }
    if (ent == &ns->pending_pipeline_ops){
        int written = sprintf(ns->output_buf, 
                "ERROR: pipeline op with id %d not found", 
                m->u.palloc_callback.id.op_id);
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
        return;
    }

    int tid = m->u.palloc_callback.id.tid;

    // assign chunk id (giving us an offset for lsm) and chunk size
    // first, check if thread has already been allocated
    // NOTE: need to check before making any state changes to properly roll
    // back
    if (qi->req->threads[tid].chunk_id != -1){
        int written = sprintf(ns->output_buf,
                "ERROR: thread %d already allocated its buffer", tid);
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
        return;
    }

    qi->req->nthreads_alloc_waiting--;

    if (qi->req->rem > 0){
        b->c0 = 1;
        // set up and send out the request
        int chunk_id = qi->req->thread_chunk_id_curr++;
        uint64_t sz = minu64(qi->req->rem, qi->req->threads[tid].punit_size);
        qi->req->threads[tid].chunk_id = chunk_id;
        qi->req->threads[tid].chunk_size = sz;

        lprintf("%lu: alloc-cb rem:%lu-%lu\n", lp->gid, qi->req->rem,
                sz);
        qi->req->rem -= sz;

        if (qi->type == REQ_WRITE) {
            // create network ack callback
            triton_rosd_msg m_recv;
            // note: in the header, we actually want the src LP to be the
            // client LP rather than our own
            msg_set_header(rosd_magic, RECV_CHUNK, qi->cli_lp, &m_recv.h);
            m_recv.u.recv_chunk.id.op_id = qi->op_id;
            m_recv.u.recv_chunk.id.tid = tid;
            // issue "pull" of data from client
            int prio = 0;
            model_net_set_msg_param(MN_MSG_PARAM_SCHED, MN_SCHED_PARAM_PRIO,
                    (void*) &prio);
            model_net_pull_event(model_net_id, "rosd", qi->src_lp,
                    sz, 0.0, sizeof(triton_rosd_msg), &m_recv, lp);
        }
        else if (qi->type == REQ_READ) {
            // direct read
            tw_event *e = lsm_event_new(
                    ROSD_LP_NM,
                    lp->gid,
                    qi->req->req.oid,
                    qi->req->punit_size * chunk_id + qi->req->req.xfer_offset,
                    sz,
                    LSM_READ_REQUEST,
                    sizeof(triton_rosd_msg),
                    lp,
                    0.0);
            triton_rosd_msg *m_cb = lsm_event_data(e);
            msg_set_header(rosd_magic, COMPLETE_DISK_OP, lp->gid, &m_cb->h);
            m_cb->u.complete_sto.id.op_id = qi->op_id;
            m_cb->u.complete_sto.id.tid = tid;
            m_cb->u.complete_sto.is_data_op = 1;
            tw_event_send(e);
        }
        else { assert(0); }

        tprintf("%lu,%d: thread %d alloc'd before compl with chunk %d "
                "(%d+1, %d-1, %d)\n",
                lp->gid, qi->op_id, tid, chunk_id, qi->req->nthreads_init,
                qi->req->nthreads_alloc_waiting+1, qi->req->nthreads_fin);

        // if we're the last thread to process, then set the rest to a
        // finished state (see NOTE below)
        if (qi->req->rem == 0){
            b->c1 = 1;
            m->u.palloc_callback.rc.nthreads_init = qi->req->nthreads_init;
            m->u.palloc_callback.rc.nthreads_fin = qi->req->nthreads_fin;
            // finish all remaining *non*-initialized threads (but not me)
            qi->req->nthreads_fin += qi->req->nthreads -
                qi->req->nthreads_init;
            qi->req->nthreads_init = qi->req->nthreads;
        }
        // more threads to allocate
        else if (qi->req->nthreads_init < qi->req->nthreads){
            b->c2 = 1;
            pipeline_alloc_event(b, lp, m->u.palloc_callback.id.op_id,
                    qi->req);
        }
        // else nothing to do
    }
    else{
        // de-allocate if all pending data scheduled for pulling before this
        // thread got the alloc
        resource_lp_free(qi->req->threads[tid].punit_size, lp);

        // NOTE: normally we'd kick off other allocation requests, but in this
        // case we're not. Hence, we need to set the thread counts up here as if
        // the remaining threads were kicked off and didn't do anything. 
        // This is necessary because finalization code works based on thread
        // counts being in certain states.

        tprintf("%lu,%d: thread %d alloc'd after compl, "
                "tid counts (%d+%d, %d-1, %d+%d)\n",
                lp->gid, qi->op_id, tid, qi->req->nthreads_init, 
                qi->req->nthreads - qi->req->nthreads_init,
                qi->req->nthreads_alloc_waiting+1, qi->req->nthreads_fin,
                qi->req->nthreads - qi->req->nthreads_init+1);

        // set rc vars
        m->u.palloc_callback.rc.nthreads_init = qi->req->nthreads_init;
        m->u.palloc_callback.rc.nthreads_fin = qi->req->nthreads_fin;
        // finish all remaining *non*-initialized threads (*and* me)
        qi->req->nthreads_fin += qi->req->nthreads - qi->req->nthreads_init + 1;
        qi->req->nthreads_init = qi->req->nthreads;

        // if I'm the last allocator, then we are done with this operation
        // TODO: what do we do with thread counts?
        if (qi->req->nthreads_fin == qi->req->nthreads) {
            // completely finished with request, deq it
            qlist_del(&qi->ql);
            // add to statistics
            if (qi->type == REQ_WRITE) {
                ns->bytes_written_local += qi->req->committed;
                uint64_t mult = (fwd_mode == FWD_FAN) ? replication_factor-1 : 1;
                ns->bytes_forwarded += qi->req->forwarded * mult;
            }
            else {
                ns->bytes_read_local += qi->req->committed;
                ns->bytes_returned += qi->req->forwarded;
            }
            lprintf("%lu: rm req %d (alloc_callback)\n", lp->gid, qi->op_id);
            // RC: hold on to queued-up item (TODO: mem mgmt)
            rc_stack_push(lp, qi, ns->finished_pipeline_ops);
            b->c3 = 1;
        }
        return;
    }
}

// bitfields used:
// c0 - primary has received all of client's data and ack'd the client under 
//      primary_recv mode
// c1 - non-primary has received all of client's data
// c2 - ack sent to client under all_recv+chain mode (from tail server)
//      (assumes c1)
// c3 - data recv ack sent to primary under all_recv+fan mode
//      (assumes c1)
void handle_recv_chunk(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    // first look up pipeline request
    struct qlist_head *ent = NULL;
    rosd_pipeline_qitem *qi = NULL; 
    qlist_for_each(ent, &ns->pending_pipeline_ops){
        qi = qlist_entry(ent, rosd_pipeline_qitem, ql);
        if (qi->op_id == m->u.recv_chunk.id.op_id){
            break;
        }
    }
    if (ent == &ns->pending_pipeline_ops){
        int written = sprintf(ns->output_buf, 
                "ERROR: pipeline op with id %d not found (chunk recv)", 
                m->u.recv_chunk.id.op_id);
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
        return;
    }

    int tid = m->u.recv_chunk.id.tid;
    rosd_pipelined_thread *t = &qi->req->threads[tid];
    // RC bug - allocation was reversed locally, then we got a response from the
    // client RDMA
    if (t->chunk_id == -1 || t->chunk_size == 0){
        int written = sprintf(ns->output_buf,
                "ERROR: %lu,%d: thread %d was reset and not reinitialized "
                "(id:%d sz:%lu)\n",
                lp->gid, qi->op_id, tid, t->chunk_id, t->chunk_size);
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
        return;
    }

    // get the chunk request metadata and update the received bytes information
    rosd_chunk_req_info *info = NULL;
    struct qlist_head *chunk_ent = NULL;
    if (qi->cli_lp != qi->src_lp){
        qlist_for_each(chunk_ent, &ns->pending_chunk_ops){
            info = qlist_entry(chunk_ent, rosd_chunk_req_info, ql);
            if (info->cli_src == qi->cli_lp && 
                    info->cli_op_id == qi->cli_cb.op_index) {
                info->received += t->chunk_size;
                break;
            }
        }
        if (chunk_ent == &ns->pending_chunk_ops){
            int written = sprintf(ns->output_buf, 
                    "ERROR: chunk op for cli req of lp %lu, id %lu "
                    "not found on LP %lu (recv_chunk)", 
                    qi->cli_lp, qi->cli_cb.op_index, lp->gid);
            lp_io_write(lp->gid, "errors", written, ns->output_buf);
            ns->error_ct = 1;
            return;
        }
    }
    
    lprintf("%lu: received chunk from %lu\n", lp->gid, m->h.src);

    qi->req->received += t->chunk_size;

    // issue asynchronous write, computing offset based on which chunk we're
    // using
    lprintf("%lu: writing chunk %d (cli %lu, op %lu) (oid:%lu, off:%lu, len:%lu)\n",
            lp->gid, t->chunk_id, qi->cli_lp, qi->cli_cb.op_index, qi->req->req.oid,
            qi->req->punit_size * t->chunk_id + qi->req->req.xfer_offset,
            t->chunk_size);
    tw_event *e_store = lsm_event_new(
            ROSD_LP_NM, 
            lp->gid,
            qi->req->req.oid,
            qi->req->punit_size * t->chunk_id + qi->req->req.xfer_offset,
            t->chunk_size,
            LSM_WRITE_REQUEST,
            sizeof(triton_rosd_msg),
            lp,
            0.0);
    triton_rosd_msg *m_store = lsm_event_data(e_store);
    msg_set_header(rosd_magic, COMPLETE_DISK_OP, lp->gid, &m_store->h);
    m_store->u.complete_sto.id.op_id = qi->op_id;
    m_store->u.complete_sto.id.tid = tid;
    m_store->u.complete_sto.is_data_op = 1;
    tw_event_send(e_store);

    // multiple acks/forwards can happen here
    MN_START_SEQ();

    // completion event: primary has received all of clients data in
    // primary_recv mode, causing an ack
    if (info == NULL && qi->req->received == qi->req->req.xfer_size &&
            ack_mode == ACK_PRIMARY_RECV){
        lprintf("%lu: acking to %lu\n", lp->gid, qi->cli_cb.header.src);
        triton_send_response(&qi->cli_cb, &qi->req->req, lp,
                model_net_id, ROSD_REQ_CONTROL_SZ, 0);
        b->c0 = 1;
    }
    // completion event: non-primary has received all of client's data
    else if (info != NULL && info->received == info->total) {
        b->c1 = 1;
        // If we're in chain+all-recv mode, then we can ack to the client
        // If in fan mode, then we need to send an explicit data-recv ack. 
        // In both cases, we remove the req metadata
        if (ack_mode == ACK_ALL_RECV){
            if (fwd_mode == FWD_CHAIN && 
                    qi->chain_pos == replication_factor-1){
                lprintf("%lu: acking to %lu\n", lp->gid, qi->cli_cb.header.src);
                triton_send_response(&qi->cli_cb, &qi->req->req, lp,
                        model_net_id, ROSD_REQ_CONTROL_SZ, 0);
                b->c2 = 1;
            }
            // if non-primary AND in fan mode AND acking with all_recv, then
            // explicit ack the parent (for fan+all_recv, no others need this
            // at the moment)
            else if (fwd_mode == FWD_FAN && qi->chain_pos != 0){
                triton_rosd_msg m_ack;
                msg_set_header(rosd_magic, RECV_DATA_ACK, lp->gid, &m_ack.h);
                m_ack.u.recv_data_ack.op_id = qi->rosd_cb.op_id;
                m_ack.u.recv_data_ack.chain_pos = qi->chain_pos;
                //char * cat = "srv-chunk-ack";
                int prio = 0;
                model_net_set_msg_param(MN_MSG_PARAM_SCHED, MN_SCHED_PARAM_PRIO,
                        (void*) &prio);
                model_net_event(model_net_id, "rosd", qi->src_lp,
                        ROSD_REQ_CONTROL_SZ, 0.0, sizeof(m_ack), &m_ack,
                        0, NULL, lp);
                lprintf("%lu: fan+all_recv ack to %lu (cli %lu, op %lu) "
                        "at %1.5e\n",
                        lp->gid, qi->src_lp, qi->cli_lp, qi->cli_cb.op_index,
                        tw_now(lp));
                b->c3 = 1;
            }
        }
        qlist_del(chunk_ent);
        rc_stack_push(lp, info, ns->finished_chunk_ops);
    }

    // forward the chunk to the next server
    // chain: next in line unless tail
    // fan:   primary forwards all
    int do_chain_fwd = 
        (fwd_mode == FWD_CHAIN && qi->chain_pos < replication_factor-1);
    int do_fan_fwd = (fwd_mode == FWD_FAN && qi->chain_pos == 0);
    triton_rosd_msg m_req;
    if (do_chain_fwd || do_fan_fwd){
        msg_set_header(rosd_magic, RECV_SRV_REQ, lp->gid, &m_req.h);
        // set up offset/length parameters
        // chain pos = 0 - off/len deps on chunk
        if (qi->chain_pos == 0){
            m_req.u.sreq.req.xfer_offset = t->chunk_id * qi->req->punit_size +
                qi->req->req.xfer_offset;
            m_req.u.sreq.req.xfer_size = t->chunk_size;
        }
        // chain pos > 0 - off/len already in req
        else{
            m_req.u.sreq.req.xfer_offset = qi->req->req.xfer_offset;
            m_req.u.sreq.req.xfer_size = qi->req->req.xfer_size;
        }
        // set up rest of req
        m_req.u.sreq.req.req_type = qi->req->req.req_type;
        m_req.u.sreq.req.create = qi->req->req.create;
        m_req.u.sreq.req.oid = qi->req->req.oid;
        // set up everything else
        m_req.u.sreq.callback = qi->cli_cb;
        // req sz - non-primary gets from chunk metadata 
        m_req.u.sreq.total_req_sz = 
            (qi->chain_pos==0) ? qi->req->req.xfer_size : info->total;
        m_req.u.sreq.id.op_id = qi->op_id;
        m_req.u.sreq.id.tid = tid;
        // use the placement, luke
        placement_find_closest(m_req.u.sreq.req.oid, replication_factor,
                ns->oid_srv_map);
        int num_fwds = do_chain_fwd ? 1 : replication_factor-1;
        for (int i = 1; i <= num_fwds; i++){
            tw_lpid dst = get_rosd_lpid((int)ns->oid_srv_map[i+qi->chain_pos]);
            m_req.u.sreq.chain_pos = i+qi->chain_pos;
            //char * cat = "srv-chunk-req";
            int prio = 0;
            model_net_set_msg_param(MN_MSG_PARAM_SCHED, MN_SCHED_PARAM_PRIO,
                    (void*) &prio);
            model_net_event(model_net_id, "rosd", dst,
                    ROSD_REQ_CONTROL_SZ, 0.0, sizeof(m_req), &m_req,
                    0, NULL, lp);
            lprintf("%lu: forward req to %lu\n", lp->gid, dst);
        }
    }
    // else set forwarding status for thread to a dummy
    else if (fwd_mode == FWD_CHAIN){
        t->status.fwd_status_ct = 1;
    }
    else{
        t->status.fwd_status_ct = replication_factor-1;
        for (int i = 1; i <= replication_factor-1; i++){
            t->status.fwd_status[i]=1;
        }
    }

    MN_END_SEQ();
}

// bitfields used:
// - c0 - operation fully complete, causing request to be deleted
static void handle_async_meta_completion(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    int id;
    if (m->h.event_type == COMPLETE_DISK_OP){
        id = m->u.complete_sto.id.op_id;
    }
    else{
        id = m->u.recv_meta_ack.op_id;
    }

    // find the meta op
    struct qlist_head *ent = NULL;
    rosd_meta_qitem *qi = NULL;
    qlist_for_each(ent, &ns->pending_meta_ops){
        qi = qlist_entry(ent, rosd_meta_qitem, ql);
        if (qi->op_id == id){
            break;
        }
    }
    if (ent == &ns->pending_meta_ops){
        int written = sprintf(ns->output_buf,
                "ERROR: meta op with id %d not found on LP %lu "
                "(async_meta_completion,%s)", id, lp->gid,
                m->h.event_type==COMPLETE_DISK_OP ? "disk" : "fwd");
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
        return;
    }

    // update the status
    if (m->h.event_type == COMPLETE_DISK_OP){
        if (qi->status.local_status > 0){
            int written = sprintf(ns->output_buf,
                    "ERROR: LP %lu duplicate storage acks "
                    "(op_id %d)\n",
                    lp->gid, qi->op_id);
            lp_io_write(lp->gid, "errors", written, ns->output_buf);
            ns->error_ct = 1;
            return;
        }
        qi->status.local_status++;
        // if we're the primary under primary_commit, ack
        if (ack_mode == ACK_PRIMARY_COMMIT && qi->chain_pos == 0) {
            triton_send_response(&qi->cli_cb, &qi->req, lp,
                    model_net_id, ROSD_REQ_CONTROL_SZ, 0);
        }
    }
    else {
        if (fwd_mode == FWD_CHAIN) {
            if (qi->status.fwd_status_ct > 0){
                int written = sprintf(ns->output_buf,
                        "ERROR: LP %lu duplicate forwarding acks "
                        "(op_id %d)\n",
                        lp->gid, qi->op_id);
                lp_io_write(lp->gid, "errors", written, ns->output_buf);
                ns->error_ct = 1;
                return;
            }
            qi->status.fwd_status_ct++;
        }
        else if (fwd_mode == FWD_FAN){
            int c = m->u.recv_meta_ack.chain_pos;
            if (qi->status.fwd_status[c] > 0) {
                int written = sprintf(ns->output_buf,
                        "ERROR: LP %lu duplicate forwarding acks "
                        "(op_id %d)\n",
                        lp->gid, qi->op_id);
                lp_io_write(lp->gid, "errors", written, ns->output_buf);
                ns->error_ct = 1;
                return;
            }
            qi->status.fwd_status_ct++;
            qi->status.fwd_status[c]++;
        }
        else
            assert(0);
    }

    int is_primary = (qi->chain_pos == 0);
    int is_tail = (fwd_mode == FWD_CHAIN) ?
            qi->chain_pos == replication_factor-1 :
            qi->chain_pos > 0;

    // check if op is fully complete in the eyes of the server
    // (if it's not a create then fwd unnecessary)
    // (if it's at the tail end of the chain then ignore the fwd status check)
    int is_op_complete;
    if (qi->status.local_status && is_tail)
        is_op_complete = 1;
    else if (qi->status.local_status && is_primary && !qi->req.create)
        is_op_complete = 1;
    else
        is_op_complete = qi->status.local_status &&
            ((fwd_mode == FWD_CHAIN) ? (qi->status.fwd_status_ct == 1)
             : (qi->status.fwd_status_ct == replication_factor-1));

    if (is_op_complete) {
        if (is_primary && ack_mode == ACK_ALL_COMMIT) {
            triton_send_response(&qi->cli_cb, &qi->req, lp,
                    model_net_id, ROSD_REQ_CONTROL_SZ, 0);
        }
        else if (!is_primary) {
            // need to send an internal ack to the originating server
            assert(qi->src_lp != qi->cli_lp);
            triton_rosd_msg m_ack;
            int prio = 0;
            msg_set_header(rosd_magic, RECV_METADATA_FWD_ACK, lp->gid, &m_ack.h);
            m_ack.u.recv_meta_ack.op_id = qi->rosd_cb.op_id;
            m_ack.u.recv_meta_ack.chain_pos = qi->chain_pos;
            model_net_set_msg_param(MN_MSG_PARAM_SCHED, MN_SCHED_PARAM_PRIO,
                    (void*) &prio);
            model_net_event(model_net_id, "rosd", qi->src_lp,
                    ROSD_REQ_CONTROL_SZ, 0.0, sizeof(m_ack), &m_ack, 0, NULL,
                    lp);
        }
        // this op is done, get "rid" of it
        qlist_del(&qi->ql);
        rc_stack_push(lp, qi, ns->finished_meta_ops);
        b->c0 = 1;
    }
    // else don't do anything
}

// bitfields used:
// c0 - client ack'd under primary_commit (after finishing disk op)
// c1 - operation on data chunk complete (for write op)
// c2 - assuming c1, thread had more work to do
// c3 - assuming c1, thread has no more work and req is complete
// c4 - assuming c1, last running thread -> cleanup performed
static void handle_async_completion(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    rosd_callback_id *id;
    if (m->h.event_type == COMPLETE_DISK_OP){
        id = &m->u.complete_sto.id;
    }
    else{
        id = &m->u.chunk_ack.id;
    }

    // find the pipeline op
    struct qlist_head *ent = NULL;
    rosd_pipeline_qitem *qi = NULL; 
    qlist_for_each(ent, &ns->pending_pipeline_ops){
        qi = qlist_entry(ent, rosd_pipeline_qitem, ql);
        if (qi->op_id == id->op_id){
            break;
        }
    }
    if (ent == &ns->pending_pipeline_ops){
        int written = sprintf(ns->output_buf, 
                "ERROR: pipeline op with id %d not found on LP %lu (async_completion,%s)", 
                id->op_id, lp->gid,
                m->h.event_type==COMPLETE_DISK_OP ? "disk" : "fwd");
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
        return;
    }

    MN_START_SEQ();

    rosd_pipelined_thread *t = &qi->req->threads[id->tid];

    // update the status
    if (m->h.event_type == COMPLETE_DISK_OP){
        if (t->status.local_status > 0){
            int written = sprintf(ns->output_buf,
                    "ERROR: LP %lu duplicate storage acks "
                    "(op_id %d, thread %d)\n",
                    lp->gid, qi->op_id, id->tid);
            lp_io_write(lp->gid, "errors", written, ns->output_buf);
            ns->error_ct = 1;
            return;
        }
        t->status.local_status++;
        lprintf("%lu: commited:%lu+%lu\n", lp->gid, qi->req->committed,
                t->chunk_size);
        qi->req->committed += t->chunk_size;
        // completion condition - primary_commit and all data has been
        // committed
        if (qi->type == REQ_WRITE &&
                ack_mode == ACK_PRIMARY_COMMIT && qi->chain_pos == 0 &&
                qi->req->committed == qi->req->req.xfer_size){
            b->c0 = 1;
            triton_send_response(&qi->cli_cb, &qi->req->req, lp,
                    model_net_id, ROSD_REQ_CONTROL_SZ, 0);
        }
        // go ahead and set the rc variables, just in case
        m->u.complete_sto.rc.chunk_id = t->chunk_id;
        m->u.complete_sto.rc.chunk_size = t->chunk_size;
    }
    else{
        assert(qi->type != REQ_READ);
        if (fwd_mode == FWD_CHAIN){
            if (t->status.fwd_status_ct > 0){
                int written = sprintf(ns->output_buf,
                        "ERROR: LP %lu duplicate forwarding acks "
                        "(op_id %d, thread %d)\n",
                        lp->gid, qi->op_id, id->tid);
                lp_io_write(lp->gid, "errors", written, ns->output_buf);
                ns->error_ct = 1;
                return;
            }
            t->status.fwd_status_ct++;
            qi->req->forwarded += t->chunk_size;
        }
        else if (fwd_mode == FWD_FAN){
            int chain_pos_src = m->u.chunk_ack.chain_pos;
            assert(chain_pos_src > 0 && chain_pos_src < MAX_REPLICATION);
            if (t->status.fwd_status[chain_pos_src] > 0){
                int written = sprintf(ns->output_buf,
                        "ERROR: LP %lu duplicate forwarding acks "
                        "(op_id %d, thread %d)\n",
                        lp->gid, qi->op_id, id->tid);
                lp_io_write(lp->gid, "errors", written, ns->output_buf);
                ns->error_ct = 1;
                return;
            }
            else{
                t->status.fwd_status[chain_pos_src] = 1;
                t->status.fwd_status_ct++;
                // only count as forwarded when we get response from all in fan
                if (t->status.fwd_status_ct == replication_factor-1){
                    qi->req->forwarded += t->chunk_size;
                }
            }
        }
        // go ahead and set the rc variables, just in case
        m->u.chunk_ack.rc.chunk_id = t->chunk_id;
        m->u.chunk_ack.rc.chunk_size = t->chunk_size;
    }

    if (qi->type == REQ_READ){
        /* send to client without a remote message and with a local "chunk
         * done" message */
        triton_rosd_msg m_loc;
        msg_set_header(rosd_magic, COMPLETE_CHUNK_SEND, lp->gid, &m_loc.h);
        m_loc.u.complete_chunk_send.id = *id;
        model_net_event(model_net_id, "rosd", qi->cli_lp, t->chunk_size, 0.0,
                0, NULL, sizeof(m_loc), &m_loc, lp);
    }
    else {
        // check if chunk has been fully processed
        int is_chunk_op_complete = t->status.local_status && 
            ((fwd_mode == FWD_CHAIN) ? (t->status.fwd_status_ct == 1)
             : (t->status.fwd_status_ct == replication_factor-1));

        if (is_chunk_op_complete){
            b->c1 = 1;
            // three cases to consider:
            // - thread can pull more work from src
            // - no more work to do, but there are pending chunks
            // - operation is fully complete - ack to client if primary

            // no more work to do
            if (qi->req->rem == 0){
                // if we are the primary and no pending chunks, then 
                // ack to client (under all_commit)
                // NOTE: can't simply check if we're last active thread - others
                // may be waiting on allocation still (single chunk requests)
                if (ack_mode == ACK_ALL_COMMIT && qi->chain_pos == 0 &&
                        qi->req->committed == qi->req->req.xfer_size && 
                        qi->req->forwarded == qi->req->req.xfer_size){
                    b->c3 = 1;
                    lprintf("%lu: acking to %lu\n",
                            lp->gid, qi->cli_cb.header.src);
                    triton_send_response(&qi->cli_cb, &qi->req->req, lp,
                            model_net_id, ROSD_REQ_CONTROL_SZ, 0);
                }

                // "finalize" this thread
                tprintf("%lu,%d: thread %d finished (msg %p) "
                        "tid counts (%d, %d, %d+1) (rem:0)\n",
                        lp->gid, qi->op_id, id->tid, m, qi->req->nthreads_init,
                        qi->req->nthreads_alloc_waiting, qi->req->nthreads_fin);
                qi->req->nthreads_fin++;
                resource_lp_free(t->punit_size, lp);
                // if we are the last thread then cleanup req
                if (qi->req->nthreads_fin == qi->req->nthreads){
                    // just put onto queue, as reconstructing is just too
                    // difficult ATM
                    // TODO: mem mgmt
                    lprintf("%lu: rm op %d (async compl %s)\n", lp->gid,
                            qi->op_id,
                            m->h.event_type==COMPLETE_DISK_OP ? "disk":"fwd");
                    qlist_del(&qi->ql);
                    ns->bytes_written_local += qi->req->committed;
                    uint64_t mult = (fwd_mode==FWD_FAN) ? replication_factor-1 : 1;
                    ns->bytes_forwarded += qi->req->forwarded * mult;
                    rc_stack_push(lp, qi, ns->finished_pipeline_ops);
                    b->c4 = 1;
                }
            }
            else { // more work to do
                b->c2 = 1;
                // compute new chunk size
                uint64_t chunk_sz = qi->req->punit_size > qi->req->rem ? 
                    qi->req->rem : qi->req->punit_size;
                tprintf("%lu,%d: thread %d given chunk %d (msg %p)"
                        "tid counts (%d, %d, %d) rem %lu-%lu\n",
                        lp->gid, qi->op_id, id->tid, 
                        qi->req->thread_chunk_id_curr, m,
                        qi->req->nthreads_init,
                        qi->req->nthreads_alloc_waiting, qi->req->nthreads_fin,
                        qi->req->rem, chunk_sz);
                t->chunk_id = qi->req->thread_chunk_id_curr++;
                t->chunk_size = chunk_sz;
                lprintf("%lu: async-compl rem:%lu-%lu\n", lp->gid,
                        qi->req->rem, chunk_sz);
                qi->req->rem -= chunk_sz;
                // clear out status
                t->status.local_status = 0;
                t->status.fwd_status_ct = 0;
                memset(t->status.fwd_status, 0, 
                        MAX_REPLICATION*sizeof(*t->status.fwd_status));

                // setup, send message
                triton_rosd_msg m_recv;
                msg_set_header(rosd_magic, RECV_CHUNK, qi->src_lp, &m_recv.h);
                m_recv.u.recv_chunk.id = *id;
                //char * cat = qi->chain_pos==0 ? "srv-cli-pl-pull" : "srv-srv-pl-pull";
                // control message gets high priority
                int prio = 0;
                model_net_set_msg_param(MN_MSG_PARAM_SCHED, MN_SCHED_PARAM_PRIO,
                        (void*) &prio);
                model_net_pull_event(model_net_id, "rosd", qi->src_lp, chunk_sz,
                        0.0, sizeof(triton_rosd_msg), &m_recv, lp);
                lprintf("%lu: pull req to %lu\n", lp->gid, qi->src_lp);
            }

            // if the source is a server, we need to send an internal ack
            if (qi->src_lp != qi->cli_lp){
                triton_rosd_msg m_ack;
                msg_set_header(rosd_magic, RECV_CHUNK_FWD_ACK, lp->gid, &m_ack.h);
                m_ack.u.chunk_ack.id = qi->rosd_cb;
                m_ack.u.chunk_ack.chain_pos = qi->chain_pos;
                //char * cat = "srv-chunk-ack";
                int prio = 0;
                model_net_set_msg_param(MN_MSG_PARAM_SCHED, MN_SCHED_PARAM_PRIO,
                        (void*) &prio);
                lprintf("%lu sending commit ack to %lu (cli %lu, op %lu) "
                        "at %1.5e\n",
                        lp->gid, qi->src_lp, qi->cli_lp, qi->cli_cb.op_index,
                        tw_now(lp));
                model_net_event(model_net_id, "rosd", qi->src_lp,
                        ROSD_REQ_CONTROL_SZ, 0.0, sizeof(m_ack), &m_ack, 0, NULL,
                        lp);
            }
        }
        else{
            tprintf("%lu,%d: %s op complete\n", lp->gid, id->tid,
                    m->h.event_type==COMPLETE_DISK_OP ? "loc" : "fwd");
        }
    }

    MN_END_SEQ();
}

void handle_complete_disk_op(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    if (m->u.complete_sto.is_data_op)
        handle_async_completion(ns, b, m, lp);
    else
        handle_async_meta_completion(ns, b, m, lp);
}
void handle_recv_chunk_fwd_ack(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    handle_async_completion(ns, b, m, lp);
}

void handle_recv_metadata_fwd_ack(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    handle_async_meta_completion(ns, b, m, lp);
}

void handle_recv_metadata_ack(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    assert(fwd_mode == FWD_FAN && ack_mode == ACK_ALL_RECV);

    // look up meta request
    struct qlist_head *ent = NULL;
    rosd_meta_qitem *qi = NULL;
    qlist_for_each(ent, &ns->pending_meta_ops){
        qi = qlist_entry(ent, rosd_meta_qitem, ql);
        if (qi->op_id == m->u.recv_meta_ack.op_id){
            break;
        }
    }
    if (ent == &ns->pending_meta_ops){
        int written = sprintf(ns->output_buf,
                "ERROR: meta op with id %d not found on LP %lu "
                "(meta recv ack)",
                m->u.recv_meta_ack.op_id, lp->gid);
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
        return;
    }

    // we must be the primary
    assert(qi->chain_pos == 0);

    int chain_pos_src = m->u.recv_meta_ack.chain_pos;
    assert(chain_pos_src > 0 && chain_pos_src < MAX_REPLICATION);
    if (qi->recv_status[chain_pos_src] != 0){
        int written = sprintf(ns->output_buf,
                "ERROR: %lu: already recieved meta ack for this position\n",
                lp->gid);
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
    }

    qi->recv_status[chain_pos_src] = 1;
    qi->recv_status_ct += 1;

    if (qi->recv_status_ct == replication_factor-1){
        // send a client ack
        lprintf("%lu sending ack to %lu\n", lp->gid, qi->cli_cb.header.src);
        triton_send_response(&qi->cli_cb, &qi->req, lp, model_net_id,
                ROSD_REQ_CONTROL_SZ, 0);
    }
}

// bitfields used:
// c0 - sent a response
void handle_recv_data_ack(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    // the only server that should be receiving is the primary, and only under
    // fan+all_recv mode
    assert(fwd_mode == FWD_FAN && ack_mode == ACK_ALL_RECV);

    // look up pipeline request
    struct qlist_head *ent = NULL;
    rosd_pipeline_qitem *qi = NULL; 
    qlist_for_each(ent, &ns->pending_pipeline_ops){
        qi = qlist_entry(ent, rosd_pipeline_qitem, ql);
        if (qi->op_id == m->u.recv_data_ack.op_id){
            break;
        }
    }
    if (ent == &ns->pending_pipeline_ops){
        int written = sprintf(ns->output_buf, 
                "ERROR: pipeline op with id %d not found on LP %lu "
                "(chunk recv)", 
                m->u.recv_chunk.id.op_id, lp->gid);
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
        return;
    }

    // we must be the primary
    assert(qi->chain_pos == 0);

    int chain_pos_src = m->u.recv_data_ack.chain_pos;
    assert(chain_pos_src > 0 && chain_pos_src < MAX_REPLICATION);
    // update the status (with appropriate error checks)
    if (qi->recv_status[chain_pos_src] != 0){
        int written = sprintf(ns->output_buf,
                "ERROR: %lu: already recieved data ack for this position\n",
                lp->gid);
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
        return;
    }

    qi->recv_status[chain_pos_src] = 1;
    qi->recv_status_ct += 1;

    if (qi->recv_status_ct == replication_factor-1){
        // send a client ack
        b->c0 = 1;
        lprintf("%lu sending ack to %lu\n", lp->gid, qi->cli_cb.header.src);
        triton_send_response(&qi->cli_cb, &qi->req->req, lp, model_net_id,
                ROSD_REQ_CONTROL_SZ, 0);
    }
}

// bitfields used:
// c0 - no more work to do
// c1 - last running thread -> cleanup performed
// c2 - all data on the wire -> ack to client
void handle_complete_chunk_send(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp) {
    // find the pipeline op
    struct qlist_head *ent = NULL;
    rosd_pipeline_qitem *qi = NULL;
    rosd_callback_id id = m->u.complete_chunk_send.id;
    qlist_for_each(ent, &ns->pending_pipeline_ops){
        qi = qlist_entry(ent, rosd_pipeline_qitem, ql);
        if (qi->op_id == id.op_id){
            break;
        }
    }
    if (ent == &ns->pending_pipeline_ops){
        int written = sprintf(ns->output_buf,
                "ERROR: pipeline op with id %d not found on LP %lu "
                "(complete_chunk_send)",
                id.op_id, lp->gid);
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
        return;
    }

    rosd_pipelined_thread *t = &qi->req->threads[id.tid];
    // in either case, set chunk info for rc
    m->u.complete_chunk_send.rc.chunk_id = t->chunk_id;
    m->u.complete_chunk_send.rc.chunk_size = t->chunk_size;

    qi->req->forwarded += t->chunk_size;

    // three cases to consider:
    // - thread can read more
    // - no more work to do, but there are pending chunks to be read / sent
    // - this thread is last to send to client - ack to client

    // no more work
    if (qi->req->rem == 0){
        // "finalize" this thread
        b->c0 = 1;
        tprintf("%lu,%d: thread %d finished (msg %p) "
                "tid counts (%d, %d, %d+1) (rem:0)\n",
                lp->gid, qi->op_id, id.tid, m, qi->req->nthreads_init,
                qi->req->nthreads_alloc_waiting, qi->req->nthreads_fin);
        qi->req->nthreads_fin++;
        resource_lp_free(t->punit_size, lp);
        // if we are the last thread to send data to the client then ack
        if (qi->req->forwarded == qi->req->req.xfer_size) {
            b->c2 = 1;
            triton_send_response(&qi->cli_cb, &qi->req->req, lp,
                    model_net_id, ROSD_REQ_CONTROL_SZ, 0);
        }
        // if we are the last thread then cleanup req and ack to client
        if (qi->req->nthreads_fin == qi->req->nthreads) {
            b->c1 = 1;
            ns->bytes_read_local += qi->req->committed;
            ns->bytes_returned += qi->req->forwarded;
            lprintf("%lu: rm op %d (compl chunk send)\n", lp->gid, qi->op_id);
            qlist_del(&qi->ql);
            rc_stack_push(lp, qi, ns->finished_pipeline_ops);
        }
        // else other threads still running, do nothing
    }
    else { // more work to do
        // compute new chunk size
        uint64_t chunk_sz = qi->req->punit_size > qi->req->rem ?
            qi->req->rem : qi->req->punit_size;
        tprintf("%lu,%d: thread %d given chunk %d (msg %p)"
                "tid counts (%d, %d, %d) rem %lu-%lu\n",
                lp->gid, qi->op_id, id.tid,
                qi->req->thread_chunk_id_curr, m,
                qi->req->nthreads_init,
                qi->req->nthreads_alloc_waiting, qi->req->nthreads_fin,
                qi->req->rem, chunk_sz);
        t->chunk_id = qi->req->thread_chunk_id_curr++;
        t->chunk_size = chunk_sz;
        lprintf("%lu: compl send rem:%lu-%lu\n", lp->gid, qi->req->rem, chunk_sz);
        qi->req->rem -= chunk_sz;
        // clear out status
        t->status.local_status = 0;

        // do a read
        tw_event *e = lsm_event_new(
                ROSD_LP_NM,
                lp->gid,
                qi->req->req.oid,
                qi->req->punit_size * t->chunk_id + qi->req->req.xfer_offset,
                chunk_sz,
                LSM_READ_REQUEST,
                sizeof(triton_rosd_msg),
                lp,
                0.0);

        triton_rosd_msg *m_cb = lsm_event_data(e);
        msg_set_header(rosd_magic, COMPLETE_DISK_OP, lp->gid, &m_cb->h);
        m_cb->u.complete_sto.id = id;
        m_cb->u.complete_sto.is_data_op = 1;
        tw_event_send(e);
    }
}

// bitfields used:
// c31 - alloc not called
static void pipeline_alloc_event_rc(
        tw_bf *b, 
        tw_lp *lp, 
        int op_id,
        rosd_pipelined_req *req){
    req->nthreads_alloc_waiting--;
    req->nthreads_init--;
    resource_lp_get_rc(lp);
}

void handle_recv_io_req_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){

    // verify object status on server 
    // TODO: currently just checks against placement
    int is_obj_on_server = 0;
    placement_find_closest(m->u.creq.req.oid, replication_factor, 
            ns->oid_srv_map);
    for (int srv_map_pos = 0; srv_map_pos < replication_factor; 
            srv_map_pos++){
        if ((int)ns->oid_srv_map[srv_map_pos] == ns->server_index){
            is_obj_on_server = 1;
            break;
        }
    }
    if (!is_obj_on_server){
        tw_error(TW_LOC, 
                "object not resident on server, TODO: handle properly\n");
    }

    int op_id_prev = (m->h.event_type==RECV_CLI_REQ) ?
        m->u.creq.rc.op_id : m->u.sreq.rc.op_id;

    if (m->u.creq.req.req_type == REQ_OPEN) {
        // find the related request or die trying
        rosd_meta_qitem *qm = NULL;
        struct qlist_head *ent;
        assert(!qlist_empty(&ns->pending_meta_ops));
        qlist_for_each(ent, &ns->pending_meta_ops) {
            qm = qlist_entry(ent, rosd_meta_qitem, ql);
            if (qm->op_id == op_id_prev)
                break;
        }
        assert(ent != &ns->pending_meta_ops);
        qlist_del(ent);

        lsm_event_new_reverse(lp);

        if (m->h.event_type == RECV_CLI_REQ) {
            if (ack_mode == ACK_PRIMARY_RECV)
                triton_send_response_rev(lp, model_net_id, ROSD_REQ_CONTROL_SZ);
            if (replication_factor > 1 && qm->req.create) {
                int i;
                int num_fwds = (fwd_mode == FWD_FAN) ? replication_factor-1 : 1;
                for (i = 0; i < num_fwds; i++) {
                    model_net_event_rc(model_net_id, lp, ROSD_REQ_CONTROL_SZ);
                }
            }
        }
        else {
            if (fwd_mode == FWD_CHAIN) {
                if (qm->chain_pos < replication_factor-1 && qm->req.create)
                    model_net_event_rc(model_net_id, lp, ROSD_REQ_CONTROL_SZ);
                if (qm->chain_pos == replication_factor-1 &&
                        ack_mode == ACK_ALL_RECV)
                    triton_send_response_rev(lp, model_net_id, ROSD_REQ_CONTROL_SZ);
            }
            if (fwd_mode == FWD_FAN && ack_mode == ACK_ALL_RECV)
                model_net_event_rc(model_net_id, lp, ROSD_REQ_CONTROL_SZ);
        }
        free(qm);
        return;
    }

    // find the queue item (removal from list can be from any location, rc
    // pushes it back to the back, so we have to iterate)
    struct qlist_head *qitem_ent;
    rosd_pipeline_qitem *qi = NULL;
    qlist_for_each(qitem_ent, &ns->pending_pipeline_ops){
        qi = qlist_entry(qitem_ent, rosd_pipeline_qitem, ql);
        if (qi->op_id == op_id_prev){
            break;
        }
    }
    assert(qitem_ent != &ns->pending_pipeline_ops);

    // before doing cleanups (free(...)), reverse the alloc event
    pipeline_alloc_event_rc(b, lp, op_id_prev, qi->req);

    // find and delete the chunk info, but only if it was created in the
    // corresponding io req
    if (m->h.event_type != RECV_CLI_REQ && b->c0) {
        rosd_chunk_req_info *id = NULL;
        struct qlist_head *chunk_ent;
        qlist_for_each(chunk_ent, &ns->pending_chunk_ops){
            id = qlist_entry(chunk_ent, rosd_chunk_req_info, ql);
            if (id->cli_src == qi->cli_lp &&
                    id->cli_op_id == qi->cli_cb.op_index){
                break;
            }
        }
        assert(chunk_ent != &ns->pending_chunk_ops);
        qlist_del(chunk_ent);
        free(id);
    }

    lprintf("%lu: new req rc id:%d from %lu\n", lp->gid, qi->op_id, qi->src_lp);
    qlist_del(qitem_ent);
    rosd_pipeline_destroy(qi->req);
    free(qi);
}

// bitfields used:
// c0 - there was data to pull post- thread allocation
// c1 - assuming  c0, thread is last to alloc due to no remaining data after
// c2 - assuming  c0 and !c1, thread was not the last thread and issued an alloc
// c3 - assuming !c0, thread is the last active, so pipeline request was deleted
//      (and moved to the finished queue)
void handle_pipeline_alloc_callback_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    // find the request 
    rosd_pipeline_qitem *qi = NULL;
    if (b->c3){
        qi = rc_stack_pop(ns->finished_pipeline_ops);
        // add back into the queue
        qlist_add_tail(&qi->ql, &ns->pending_pipeline_ops);
        // undo the stats
        if (qi->type == REQ_WRITE) {
            ns->bytes_written_local -= qi->req->committed;
            uint64_t mult = (fwd_mode==FWD_FAN) ? replication_factor-1 : 1;
            ns->bytes_forwarded -= qi->req->forwarded * mult;
        }
        else {
            ns->bytes_read_local -= qi->req->committed;
            ns->bytes_returned -= qi->req->forwarded;
        }
        lprintf("%lu: add req %d (alloc_callback rc)\n", lp->gid, qi->op_id);
    }
    else{
        struct qlist_head *ent = NULL;
        qlist_for_each(ent, &ns->pending_pipeline_ops){
            qi = qlist_entry(ent, rosd_pipeline_qitem, ql);
            if (qi->op_id == m->u.palloc_callback.id.op_id){
                break;
            }
        }
        assert(ent != &ns->pending_pipeline_ops);
    }
    
    int tid = m->u.palloc_callback.id.tid;

    if (b->c0){
        qi->req->thread_chunk_id_curr--;
        // the size computed is stored in the thread's chunk_size
        uint64_t sz = qi->req->threads[tid].chunk_size;
        lprintf("%lu: alloc-cb rc rem:%lu+%lu\n", lp->gid, qi->req->rem, sz);
        qi->req->rem += sz;
        qi->req->threads[tid].chunk_size = 0;
        qi->req->threads[tid].chunk_id = -1;

        if (qi->type == REQ_WRITE)
            model_net_pull_event_rc(model_net_id, lp);
        else if (qi->type == REQ_READ)
            lsm_event_new_reverse(lp);
        else { assert(0); }

        tprintf("%lu,%d: thread %d alloc'd before compl with chunk %d rc, "
                "tid counts (%d-1, %d+1, %d)\n",
                lp->gid, qi->op_id, tid, qi->req->thread_chunk_id_curr,
                qi->req->nthreads_init, qi->req->nthreads_alloc_waiting,
                qi->req->nthreads_fin);
        if (b->c1){
            qi->req->nthreads_init = m->u.palloc_callback.rc.nthreads_init;
            qi->req->nthreads_fin  = m->u.palloc_callback.rc.nthreads_fin;
        }
        else if (b->c2){
            pipeline_alloc_event_rc(b, lp, qi->op_id, qi->req);
        }
    }
    else{
        resource_lp_free_rc(lp);
        tprintf("%lu,%d: thread %d alloc'd after compl rc, "
                "tid counts (%d-%d, %d+1, %d-%d)\n",
                lp->gid, qi->op_id, tid, qi->req->nthreads_init,
                qi->req->nthreads_init -
                m->u.palloc_callback.rc.nthreads_init,
                qi->req->nthreads_alloc_waiting, qi->req->nthreads_fin,
                qi->req->nthreads_fin -
                m->u.palloc_callback.rc.nthreads_fin);
        qi->req->nthreads_init = m->u.palloc_callback.rc.nthreads_init;
        qi->req->nthreads_fin  = m->u.palloc_callback.rc.nthreads_fin;
        // note: undid the queue deletion at beginning of RC
    }
    qi->req->nthreads_alloc_waiting++;
}
void handle_recv_chunk_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    // first look up pipeline request
    struct qlist_head *ent = NULL;
    rosd_pipeline_qitem *qi = NULL; 
    qlist_for_each(ent, &ns->pending_pipeline_ops){
        qi = qlist_entry(ent, rosd_pipeline_qitem, ql);
        if (qi->op_id == m->u.recv_chunk.id.op_id){
            break;
        }
    }
    assert(ent != &ns->pending_pipeline_ops);

    int tid = m->u.recv_chunk.id.tid;
    rosd_pipelined_thread *t = &qi->req->threads[tid];

    lsm_event_new_reverse(lp);

    qi->req->received -= t->chunk_size;

    // find the chunk request metadata
    rosd_chunk_req_info *info = NULL;
    struct qlist_head *chunk_ent = NULL;
    if (qi->cli_lp != qi->src_lp){
        if (b->c1){
            info = rc_stack_pop(ns->finished_chunk_ops);
            chunk_ent = &info->ql;
            qlist_add_tail(chunk_ent, &ns->pending_chunk_ops);
        }
        else{
            qlist_for_each(chunk_ent, &ns->pending_chunk_ops){
                info = qlist_entry(chunk_ent, rosd_chunk_req_info, ql);
                if (info->cli_src == qi->cli_lp
                        && info->cli_op_id == qi->cli_cb.op_index) {
                    break;
                }
            }
        }
        // TODO: duplicate recv error
        assert(chunk_ent != &ns->pending_chunk_ops);
        info->received -= t->chunk_size;
    }

    if (b->c0){
        triton_send_response_rev(lp, model_net_id, ROSD_REQ_CONTROL_SZ);
    }
    else if (b->c2){
        triton_send_response_rev(lp, model_net_id, ROSD_REQ_CONTROL_SZ);
    }
    else if (b->c3){
        model_net_event_rc(model_net_id, lp, ROSD_REQ_CONTROL_SZ);
    }

    int do_chain_fwd = 
        (fwd_mode == FWD_CHAIN && qi->chain_pos < replication_factor-1);
    int do_fan_fwd = (fwd_mode == FWD_FAN && qi->chain_pos == 0);

    if (do_chain_fwd || do_fan_fwd){
        int num_fwds = do_chain_fwd ? 1 : replication_factor-1;
        for (int i = 1; i <= num_fwds; i++){
            model_net_event_rc(model_net_id, lp, ROSD_REQ_CONTROL_SZ);
        }
    }
    // need to reset dummy forwarding status
    else if (fwd_mode == FWD_CHAIN){
        t->status.fwd_status_ct = 0;
    }
    else{
        t->status.fwd_status_ct = 0;
        memset(t->status.fwd_status+1, 0, 
                (replication_factor-1)*sizeof(*t->status.fwd_status));
    }
}

static void handle_async_meta_completion_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    int id;
    if (m->h.event_type == COMPLETE_DISK_OP){
        id = m->u.complete_sto.id.op_id;
    }
    else{
        id = m->u.recv_meta_ack.op_id;
    }

    rosd_meta_qitem *qi;
    if (b->c0) {
        qi = rc_stack_pop(ns->finished_meta_ops);
        assert(qi != NULL);
    }
    else {
        struct qlist_head *ent;
        qlist_for_each(ent, &ns->pending_meta_ops) {
            qi = qlist_entry(ent, rosd_meta_qitem, ql);
            if (qi->op_id == id)
                break;
        }
        assert(ent != &ns->pending_meta_ops);
    }

    int is_primary = (qi->chain_pos == 0);
    if (b->c0) {
        if (is_primary && ack_mode == ACK_ALL_COMMIT)
            triton_send_response_rev(lp, model_net_id, ROSD_REQ_CONTROL_SZ);
        else if (!is_primary)
            model_net_event_rc(model_net_id, lp, ROSD_REQ_CONTROL_SZ);
        // put the op back on the pending list
        qlist_add_tail(&qi->ql, &ns->pending_meta_ops);
    }

    // finally reverse the completion status
    if (m->h.event_type == COMPLETE_DISK_OP) {
        qi->status.local_status--;
        if (ack_mode == ACK_PRIMARY_COMMIT && qi->chain_pos == 0)
            triton_send_response_rev(lp, model_net_id, ROSD_REQ_CONTROL_SZ);
    }
    else {
        if (fwd_mode == FWD_CHAIN)
            qi->status.fwd_status_ct--;
        else if (fwd_mode == FWD_FAN) {
            qi->status.fwd_status_ct--;
            qi->status.fwd_status[m->u.recv_meta_ack.chain_pos]--;
        }
    }
}

static void handle_async_completion_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    rosd_callback_id *id;
    int prev_chunk_id;
    uint64_t prev_chunk_size;
    if (m->h.event_type == COMPLETE_DISK_OP){
        id = &m->u.complete_sto.id;
    }
    else{
        id = &m->u.chunk_ack.id;
    }

    // get the operation
    rosd_pipeline_qitem *qi = NULL;
    if (b->c4){
        // request was "deleted", put back into play
        qi = rc_stack_pop(ns->finished_pipeline_ops);
        lprintf("%lu: add op %d (async compl %s rc)\n", lp->gid, qi->op_id,
                m->h.event_type==COMPLETE_DISK_OP ? "disk" : "fwd");
        qlist_add_tail(&qi->ql, &ns->pending_pipeline_ops);
        // undo the stats
        ns->bytes_written_local -= qi->req->committed;
        uint64_t mult = (fwd_mode==FWD_FAN) ? replication_factor-1 : 1;
        ns->bytes_forwarded -= qi->req->forwarded * mult;
    }
    else{
        struct qlist_head *ent = NULL;
        qlist_for_each(ent, &ns->pending_pipeline_ops){
            qi = qlist_entry(ent, rosd_pipeline_qitem, ql);
            if (qi->op_id == id->op_id){
                break;
            }
        }
        assert(ent != &ns->pending_pipeline_ops);
    }

    rosd_pipelined_thread *t = &qi->req->threads[id->tid];

    // set the chunk rc parameters
    if (!b->c2){ //chunk size wasn't overwritten, grab it from the thread
        prev_chunk_size = t->chunk_size;
        prev_chunk_id = -1; // unused
    }
    else if (m->h.event_type == COMPLETE_DISK_OP){
        prev_chunk_size = m->u.complete_sto.rc.chunk_size;
        prev_chunk_id   = m->u.complete_sto.rc.chunk_id;
    }
    else{
        prev_chunk_size = m->u.chunk_ack.rc.chunk_size;
        prev_chunk_id   = m->u.chunk_ack.rc.chunk_id;
    }

    // recreate the "finished" request status if needed
    if (b->c1 && b->c2){
        t->status.local_status = 1;
        if (fwd_mode == FWD_CHAIN){
            t->status.fwd_status_ct = 1;
        }
        else{
            t->status.fwd_status_ct = replication_factor-1;
            for (int i = 1; i <= replication_factor-1; i++){
                t->status.fwd_status[i] = 1;
            }
        }
    }

    // reverse the status completion
    if (m->h.event_type == COMPLETE_DISK_OP){
        lprintf("%lu: commit:%lu-%lu\n", lp->gid,
                qi->req->committed, prev_chunk_size);
        qi->req->committed -= prev_chunk_size;
        t->status.local_status = 0;
        if (b->c0){
            triton_send_response_rev(lp, model_net_id, ROSD_REQ_CONTROL_SZ);
        }
    }
    else if (fwd_mode == FWD_CHAIN){
        t->status.fwd_status_ct--;
        qi->req->forwarded -= prev_chunk_size;
    }
    else{
        // fan mode
        int chain_pos_src = m->u.chunk_ack.chain_pos;
        if (t->status.fwd_status_ct == replication_factor-1){
            qi->req->forwarded -= prev_chunk_size;
        }
        
        t->status.fwd_status_ct--;
        t->status.fwd_status[chain_pos_src] = 0;
    }

    if (qi->type == REQ_READ){
        model_net_event_rc(model_net_id, lp, prev_chunk_size);
    }
    // else write && chunk op is complete
    else if (b->c1){
        // thread pulled more data
        if (b->c2){
            // note - previous derived chunk size is currently held in thread
            tprintf("%lu,%d: thread %d given chunk rc %d (msg %p), "
                    "tid counts (%d, %d, %d), rem %lu+%lu\n",
                    lp->gid, qi->op_id, id->tid, t->chunk_id,
                    m,
                    qi->req->nthreads_init,
                    qi->req->nthreads_alloc_waiting, 
                    qi->req->nthreads_fin,
                    qi->req->rem, t->chunk_size);
            lprintf("%lu: async-compl rc rem:%lu+%lu\n", lp->gid,
                    qi->req->rem, t->chunk_size);
            qi->req->rem += t->chunk_size;
            qi->req->thread_chunk_id_curr--;
            t->chunk_size = prev_chunk_size;
            t->chunk_id   = prev_chunk_id;

            // we 'un-cleared' earlier, so nothing to do here

            model_net_pull_event_rc(model_net_id, lp);
        }
        else{
            if (b->c3){
                triton_send_response_rev(lp, model_net_id, ROSD_REQ_CONTROL_SZ);
            }
            tprintf("%lu,%d: thread %d finished rc (msg %p), "
                    "tid counts (%d, %d, %d-1)\n",
                    lp->gid, qi->op_id, id->tid, m, qi->req->nthreads_init,
                    qi->req->nthreads_alloc_waiting, qi->req->nthreads_fin);
            qi->req->nthreads_fin--;
            resource_lp_free_rc(lp);
            // reversal of deletion occurred earlier
        }

        if (qi->src_lp != qi->cli_lp){
            model_net_event_rc(model_net_id, lp, ROSD_REQ_CONTROL_SZ);
        }
    }
}

void handle_complete_disk_op_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    if (m->u.complete_sto.is_data_op)
        handle_async_completion_rc(ns, b, m, lp);
    else
        handle_async_meta_completion_rc(ns, b, m, lp);
}
void handle_recv_chunk_fwd_ack_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    handle_async_completion_rc(ns, b, m, lp);
}

void handle_recv_metadata_fwd_ack_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    handle_async_meta_completion_rc(ns, b, m, lp);
}

void handle_recv_metadata_ack_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){

    assert(fwd_mode == FWD_FAN && ack_mode == ACK_ALL_RECV);

    // look up meta request
    struct qlist_head *ent = NULL;
    rosd_meta_qitem *qi = NULL;
    qlist_for_each(ent, &ns->pending_meta_ops){
        qi = qlist_entry(ent, rosd_meta_qitem, ql);
        if (qi->op_id == m->u.recv_meta_ack.op_id){
            break;
        }
    }
    assert(ent != &ns->pending_meta_ops);

    assert(qi->chain_pos == 0);

    int chain_pos_src = m->u.recv_meta_ack.chain_pos;
    assert(chain_pos_src > 0 && chain_pos_src < MAX_REPLICATION);

    if (qi->recv_status_ct == replication_factor-1)
        triton_send_response_rev(lp, model_net_id, ROSD_REQ_CONTROL_SZ);

    qi->recv_status[chain_pos_src] = 0;
    qi->recv_status_ct--;
}

void handle_recv_data_ack_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    struct qlist_head *ent;
    rosd_pipeline_qitem *qi = NULL;
    qlist_for_each(ent, &ns->pending_pipeline_ops){
        qi = qlist_entry(ent, rosd_pipeline_qitem, ql);
        if (qi->op_id == m->u.recv_data_ack.op_id){
            break;
        }
    }
    assert(ent != &ns->pending_pipeline_ops);

    int chain_pos_src = m->u.recv_data_ack.chain_pos;
    qi->recv_status[chain_pos_src] = 0;
    qi->recv_status_ct--;

    if (b->c0){
        triton_send_response_rev(lp, model_net_id, ROSD_REQ_CONTROL_SZ);
    }
}

void handle_complete_chunk_send_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp) {
    rosd_callback_id id = m->u.complete_chunk_send.id;

    // get the operation
    rosd_pipeline_qitem *qi = NULL;

    if (b->c1) {
        // request was "deleted", put back into play
        qi = rc_stack_pop(ns->finished_pipeline_ops);
        lprintf("%lu: add op %d (compl chunk send rc)\n", lp->gid, qi->op_id);
        qlist_add_tail(&qi->ql, &ns->pending_pipeline_ops);
        // undo the stats
        ns->bytes_read_local -= qi->req->committed;
    }
    else{
        struct qlist_head *ent = NULL;
        qlist_for_each(ent, &ns->pending_pipeline_ops){
            qi = qlist_entry(ent, rosd_pipeline_qitem, ql);
            if (qi->op_id == id.op_id){
                break;
            }
        }
        assert(ent != &ns->pending_pipeline_ops);
    }

    rosd_pipelined_thread *t = &qi->req->threads[id.tid];

    if (b->c0) {
        qi->req->nthreads_fin--;
        resource_lp_free_rc(lp);
        if (b->c2)
            triton_send_response_rev(lp, model_net_id, ROSD_REQ_CONTROL_SZ);
        if (b->c1) {
            ns->bytes_read_local -= qi->req->committed;
            ns->bytes_returned -= qi->req->forwarded;
        }
    }
    else {
        qi->req->rem += t->chunk_size;
        t->status.local_status = 1;
        qi->req->thread_chunk_id_curr--;
        lsm_event_new_reverse(lp);
    }

    t->chunk_id = m->u.complete_chunk_send.rc.chunk_id;
    t->chunk_size = m->u.complete_chunk_send.rc.chunk_size;
    qi->req->forwarded -= t->chunk_size;
}

int is_rosd_valid_fwd_config(rosd_fwd_mode f, rosd_ack_mode a){
    return f < NUM_FWD_MODES && a < NUM_ACK_MODES;
}

int rosd_get_ack_mode(char const * mode, rosd_ack_mode *a){
    int i;
    for (i = 0; i < NUM_ACK_MODES; i++){
        if (strcmp(mode, ack_mode_strs[i]) == 0){
            *a = i;
            return 0;
        }
    }
    return 1;
}

int rosd_get_fwd_mode(char const * mode, rosd_fwd_mode *f){
    int i;
    for (i = 0; i < NUM_FWD_MODES; i++){
        if (strcmp(mode, fwd_mode_strs[i]) == 0){
             *f = i;
             return 0;
        }
    }
    return 1;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
