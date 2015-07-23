/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <string.h>
#include <assert.h>
#include <codes/model-net.h>
#include <codes/codes_mapping.h>
#include <codes/lp-type-lookup.h>
#include <codes/jenkins-hash.h>
#include <codes/codes.h>
#include <codes/lp-io.h>
#include <codes/local-storage-model.h>
#include <codes/model-net-sched.h>
#include <codes/rc-stack.h>
#include <codes/codes-callback.h>

#include "rosd.h"
#include "rosd-creq.h"
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

static int mn_id;

/* system parameters */
static int num_threads = 4;
static int pipeline_unit_size = (1<<22);

/* disable lp output unless told otherwise */
int do_rosd_lp_io = 0;
lp_io_handle rosd_io_handle;

/* for rc-stack: free fn for pipeline_qitem */
static void free_qitem(void * ptr)
{
    rosd_qitem *qi = ptr;
    if (qi->preq) rosd_pipeline_destroy(qi->preq);
    free(qi);
}

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

void rosd_configure(int model_net_id){
    uint32_t h1=0, h2=0;

    bj_hashlittle2(ROSD_LP_NM, strlen(ROSD_LP_NM), &h1, &h2);
    rosd_magic = h1+h2;

    mn_id = model_net_id;

    /* until we get rid of RF logic... */
    int rc;

    // get the number of threads and the pipeline buffer size
    // if not available, no problem - use a default of 4 threads, 4MB per
    // thread
    rc = configuration_get_value_int(&config, ROSD_LP_NM, "req_threads", 
            NULL, &num_threads);
    rc = configuration_get_value_int(&config, ROSD_LP_NM, "thread_buf_sz",
            NULL, &pipeline_unit_size);

    /* done!!! */
}

///// BEGIN LP, EVENT PROCESSING FUNCTION DEFS /////

// just putting these here for now, will move later...
void codes_store_send_resp(
        int rc,
        struct codes_cb_params const * p,
        tw_lp *lp)
{
    // i'm a terrible person for using VLAs and everyone should know it
    char data[p->info.event_size];

    tw_lpid cli_lp = p->h.src;

    msg_header *h = (msg_header*)(data + p->info.header_offset);
    *h = p->h;
    h->src = lp->gid;

    int *tag = (int*)(data + p->info.tag_offset);
    *tag = p->tag;

    int *cli_rc  = (int*)(data + p->info.cb_ret_offset);
    *cli_rc = rc;

    int prio = 0;
    model_net_set_msg_param(MN_MSG_PARAM_SCHED, MN_SCHED_PARAM_PRIO,
            (void*) &prio);
    model_net_event(mn_id, "rosd", cli_lp, ROSD_REQ_CONTROL_SZ, 0.0,
            p->info.event_size, data, 0, NULL, lp);
}

void codes_store_send_resp_rc(tw_lp *lp)
{
    model_net_event_rc(mn_id, lp, ROSD_REQ_CONTROL_SZ);
}


void triton_rosd_init(triton_rosd_state *ns, tw_lp *lp) {
    ns->server_index = get_rosd_index(lp->gid);

    INIT_QLIST_HEAD(&ns->pending_ops);
    rc_stack_create(&ns->finished_ops);

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
    rc_stack_gc(lp, ns->finished_ops);

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
        case COMPLETE_DISK_OP:
            handle_complete_disk_op(ns, b, m, lp);
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
        case COMPLETE_DISK_OP:
            handle_complete_disk_op_rc(ns, b, m, lp);
            break;
        case COMPLETE_CHUNK_SEND:
            handle_complete_chunk_send_rc(ns, b, m, lp);
            break;
        default:
            tw_error(TW_LOC, "unknown rosd event type");
    }
}

void triton_rosd_finalize(triton_rosd_state *ns, tw_lp *lp) {
    rc_stack_destroy(ns->finished_ops);

    // check for pending operations that did not complete or were not removed
    struct qlist_head *ent;
    qlist_for_each(ent, &ns->pending_ops){
        rosd_qitem *qi = qlist_entry(ent, rosd_qitem, ql);
        fprintf(stderr, "WARNING: LP %lu with incomplete qitem "
                "(cli lp %lu, op id %d)\n",
                lp->gid, qi->cli_cb.h.src, qi->op_id);
    }
    int written = 0;
    if (ns->server_index == 0){
        written = sprintf(ns->output_buf, 
                "# Format: <server id> <LP id> bytes <read> <written>"
#if ROSD_PRINT_RNG == 1
                " <rng model> <rng codes>"
#endif
                "\n");
    }
    written += sprintf(ns->output_buf+written,
            "%d %lu %lu %lu"
#if ROSD_PRINT_RNG == 1
            " %lu %lu"
#endif
            "\n",
            ns->server_index, lp->gid,
            ns->bytes_read_local, ns->bytes_written_local
#if ROSD_PRINT_RNG == 1
            , lp->rng[0].count, lp->rng[1].count
#endif
            );
    lp_io_write(lp->gid, "rosd-stats", written, ns->output_buf);
}

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

void handle_recv_io_req(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){

    // initialize a pipelining operation
    rosd_qitem *qi = malloc(sizeof(rosd_qitem)); 
    qi->op_id = ns->op_idx_pl++;
    qi->req = m->u.creq.req;
    qi->cli_cb = m->u.creq.callback;
    qi->preq = NULL;
    qlist_add_tail(&qi->ql, &ns->pending_ops);

    // save op id for rc
    m->u.creq.rc.op_id = qi->op_id;

    lprintf("%lu: new req id:%d from %lu\n", lp->gid, qi->op_id,
            m->h.src);

    /* metadata op - avoid the pipelining events */
    if (m->u.creq.req.type == CSREQ_OPEN
            || m->u.creq.req.type == CSREQ_CREATE) {
        tw_event *e_local;
        triton_rosd_msg *m_local;
        rosd_callback_id cb_id;
        int is_create = m->u.creq.req.type == CSREQ_CREATE;

        cb_id.op_id = qi->op_id;
        cb_id.tid = -1;

        /* in both cases, send the local disk op */
        e_local = lsm_event_new("rosd", lp->gid, qi->req.oid, 0, 0,
                is_create ? LSM_WRITE_REQUEST : LSM_READ_REQUEST,
                sizeof(*m_local), lp, 0.0);
        m_local = lsm_event_data(e_local);
        msg_set_header(rosd_magic, COMPLETE_DISK_OP, lp->gid, &m_local->h);
        m_local->u.complete_sto.id = cb_id;
        m_local->u.complete_sto.is_data_op = 0;
        tw_event_send(e_local);
    }
    else {
        qi->preq = rosd_pipeline_init(num_threads, pipeline_unit_size,
                qi->req.xfer_size);

        // send the initial allocation event 
        pipeline_alloc_event(b, lp, qi->op_id, qi->preq);
    }
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
    rosd_qitem *qi = NULL; 
    qlist_for_each(ent, &ns->pending_ops){
        qi = qlist_entry(ent, rosd_qitem, ql);
        if (qi->op_id == m->u.palloc_callback.id.op_id){
            break;
        }
    }
    if (ent == &ns->pending_ops){
        int written = sprintf(ns->output_buf, 
                "ERROR: pipeline op with id %d not found", 
                m->u.palloc_callback.id.op_id);
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
        return;
    }

    int tid = m->u.palloc_callback.id.tid;

    rosd_pipelined_req *p = qi->preq;

    // assign chunk id (giving us an offset for lsm) and chunk size
    // first, check if thread has already been allocated
    // NOTE: need to check before making any state changes to properly roll
    // back
    if (p->threads[tid].chunk_id != -1){
        int written = sprintf(ns->output_buf,
                "ERROR: thread %d already allocated its buffer", tid);
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
        return;
    }

    p->nthreads_alloc_waiting--;

    if (p->rem > 0){
        b->c0 = 1;
        // set up and send out the request
        int chunk_id = p->thread_chunk_id_curr++;
        uint64_t sz = minu64(p->rem, p->threads[tid].punit_size);
        p->threads[tid].chunk_id = chunk_id;
        p->threads[tid].chunk_size = sz;

        lprintf("%lu: alloc-cb rem:%lu-%lu\n", lp->gid, p->rem,
                sz);
        p->rem -= sz;

        if (qi->req.type == CSREQ_WRITE) {
            // create network ack callback
            triton_rosd_msg m_recv;
            // note: in the header, we actually want the src LP to be the
            // client LP rather than our own
            msg_set_header(rosd_magic, RECV_CHUNK, qi->cli_cb.h.src, &m_recv.h);
            m_recv.u.recv_chunk.id.op_id = qi->op_id;
            m_recv.u.recv_chunk.id.tid = tid;
            // issue "pull" of data from client
            int prio = 0;
            model_net_set_msg_param(MN_MSG_PARAM_SCHED, MN_SCHED_PARAM_PRIO,
                    (void*) &prio);
            model_net_pull_event(mn_id, "rosd", qi->cli_cb.h.src,
                    sz, 0.0, sizeof(triton_rosd_msg), &m_recv, lp);
        }
        else if (qi->req.type == CSREQ_READ) {
            // direct read
            tw_event *e = lsm_event_new(
                    ROSD_LP_NM,
                    lp->gid,
                    qi->req.oid,
                    p->punit_size * chunk_id + qi->req.xfer_offset,
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
                lp->gid, qi->op_id, tid, chunk_id, p->nthreads_init,
                p->nthreads_alloc_waiting+1, p->nthreads_fin);

        // if we're the last thread to process, then set the rest to a
        // finished state (see NOTE below)
        if (p->rem == 0){
            b->c1 = 1;
            m->u.palloc_callback.rc.nthreads_init = p->nthreads_init;
            m->u.palloc_callback.rc.nthreads_fin = p->nthreads_fin;
            // finish all remaining *non*-initialized threads (but not me)
            p->nthreads_fin += p->nthreads - p->nthreads_init;
            p->nthreads_init = p->nthreads;
        }
        // more threads to allocate
        else if (p->nthreads_init < p->nthreads){
            b->c2 = 1;
            pipeline_alloc_event(b, lp, m->u.palloc_callback.id.op_id, p);
        }
        // else nothing to do
    }
    else{
        // de-allocate if all pending data scheduled for pulling before this
        // thread got the alloc
        resource_lp_free(p->threads[tid].punit_size, lp);

        // NOTE: normally we'd kick off other allocation requests, but in this
        // case we're not. Hence, we need to set the thread counts up here as if
        // the remaining threads were kicked off and didn't do anything. 
        // This is necessary because finalization code works based on thread
        // counts being in certain states.

        tprintf("%lu,%d: thread %d alloc'd after compl, "
                "tid counts (%d+%d, %d-1, %d+%d)\n",
                lp->gid, qi->op_id, tid, p->nthreads_init, 
                p->nthreads - p->nthreads_init,
                p->nthreads_alloc_waiting+1, p->nthreads_fin,
                p->nthreads - p->nthreads_init+1);

        // set rc vars
        m->u.palloc_callback.rc.nthreads_init = p->nthreads_init;
        m->u.palloc_callback.rc.nthreads_fin = p->nthreads_fin;
        // finish all remaining *non*-initialized threads (*and* me)
        p->nthreads_fin += p->nthreads - p->nthreads_init + 1;
        p->nthreads_init = p->nthreads;

        // if I'm the last allocator, then we are done with this operation
        // TODO: what do we do with thread counts?
        if (p->nthreads_fin == p->nthreads) {
            // completely finished with request, deq it
            qlist_del(&qi->ql);
            // add to statistics
            if (qi->req.type == CSREQ_WRITE) {
                ns->bytes_written_local += p->committed;
            }
            else {
                ns->bytes_read_local += p->committed;
            }
            lprintf("%lu: rm req %d (alloc_callback)\n", lp->gid, qi->op_id);
            // RC: hold on to queued-up item (TODO: mem mgmt)
            rc_stack_push(lp, qi, free_qitem, ns->finished_ops);
            b->c3 = 1;
        }
        return;
    }
}

void handle_recv_chunk(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    // first look up pipeline request
    struct qlist_head *ent = NULL;
    rosd_qitem *qi = NULL; 
    qlist_for_each(ent, &ns->pending_ops){
        qi = qlist_entry(ent, rosd_qitem, ql);
        if (qi->op_id == m->u.recv_chunk.id.op_id){
            break;
        }
    }
    if (ent == &ns->pending_ops){
        int written = sprintf(ns->output_buf, 
                "ERROR: pipeline op with id %d not found (chunk recv)", 
                m->u.recv_chunk.id.op_id);
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
        return;
    }

    int tid = m->u.recv_chunk.id.tid;
    rosd_pipelined_req *p = qi->preq;
    rosd_pipelined_thread *t = &p->threads[tid];
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

    lprintf("%lu: received chunk from %lu\n", lp->gid, m->h.src);

    p->received += t->chunk_size;

    // issue asynchronous write, computing offset based on which chunk we're
    // using
    lprintf("%lu: writing chunk %d (cli %lu, tag %d) (oid:%lu, off:%lu, len:%lu)\n",
            lp->gid, t->chunk_id,
            qi->cli_cb.h.src, qi->cli_cb.tag, qi->req.oid,
            p->punit_size * t->chunk_id + qi->req.xfer_offset,
            t->chunk_size);
    tw_event *e_store = lsm_event_new(
            ROSD_LP_NM, 
            lp->gid,
            qi->req.oid,
            p->punit_size * t->chunk_id + qi->req.xfer_offset,
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
}

// bitfields used:
// c0 - write - all committed, client ack'd
// c1 - metadata op - client ack'd, cleanup done
// c2 - thread had more work to do
// c3 - !c2, last running thread -> cleanup performed
static void handle_complete_disk_op(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    rosd_callback_id *id;
    id = &m->u.complete_sto.id;

    // find the pipeline op
    struct qlist_head *ent = NULL;
    rosd_qitem *qi = NULL; 
    qlist_for_each(ent, &ns->pending_ops){
        qi = qlist_entry(ent, rosd_qitem, ql);
        if (qi->op_id == id->op_id){
            break;
        }
    }
    if (ent == &ns->pending_ops){
        int written = sprintf(ns->output_buf, 
                "ERROR: pipeline op with id %d not found on LP %lu (async_completion,%s)", 
                id->op_id, lp->gid,
                m->h.event_type==COMPLETE_DISK_OP ? "disk" : "fwd");
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
        return;
    }

    if (!m->u.complete_sto.is_data_op) {
        codes_store_send_resp(0, &qi->cli_cb, lp);
        qlist_del(&qi->ql);
        rc_stack_push(lp, qi, free_qitem, ns->finished_ops);
        b->c1 = 1;
    }
    else {
        rosd_pipelined_req *p = qi->preq;
        rosd_pipelined_thread *t = &p->threads[id->tid];

        lprintf("%lu: committed:%lu+%lu\n", lp->gid, p->committed,
                t->chunk_size);
        p->committed += t->chunk_size;

        // go ahead and set the rc variables, just in case
        m->u.complete_sto.rc.chunk_id = t->chunk_id;
        m->u.complete_sto.rc.chunk_size = t->chunk_size;

        tw_lpid cli_lp = qi->cli_cb.h.src;

        if (qi->req.type == CSREQ_READ){
            /* send to client without a remote message and with a local "chunk
             * done" message */
            triton_rosd_msg m_loc;
            msg_set_header(rosd_magic, COMPLETE_CHUNK_SEND, lp->gid, &m_loc.h);
            m_loc.u.complete_chunk_send.id = *id;
            model_net_event(mn_id, "rosd", cli_lp, t->chunk_size, 0.0,
                    0, NULL, sizeof(m_loc), &m_loc, lp);
        }
        else {
            // two cases to consider:
            // - thread can pull more work from src
            // - no more work to do, but there are pending chunks

            // first to see all committed data acks the client
            if (p->committed == qi->req.xfer_size) {
                b->c0 = 1;
                codes_store_send_resp(0, &qi->cli_cb, lp);
            }

            // no more work to do
            if (p->rem == 0){
                // if we are the primary and no pending chunks, then 
                // ack to client (under all_commit)
                // NOTE: can't simply check if we're last active thread - others
                // may be waiting on allocation still (single chunk requests)

                // "finalize" this thread
                tprintf("%lu,%d: thread %d finished (msg %p) "
                        "tid counts (%d, %d, %d+1) (rem:0)\n",
                        lp->gid, qi->op_id, id->tid, m, p->nthreads_init,
                        p->nthreads_alloc_waiting, p->nthreads_fin);
                p->nthreads_fin++;
                resource_lp_free(t->punit_size, lp);
                // if we are the last thread then cleanup req
                if (p->nthreads_fin == p->nthreads){
                    // just put onto queue, as reconstructing is just too
                    // difficult ATM
                    // TODO: mem mgmt
                    lprintf("%lu: rm op %d (async compl %s)\n", lp->gid,
                            qi->op_id,
                            m->h.event_type==COMPLETE_DISK_OP ? "disk":"fwd");
                    qlist_del(&qi->ql);
                    ns->bytes_written_local += p->committed;
                    rc_stack_push(lp, qi, free_qitem, ns->finished_ops);
                    b->c3 = 1;
                }
            }
            else { // more work to do
                b->c2 = 1;
                // compute new chunk size
                uint64_t chunk_sz = p->punit_size > p->rem ? 
                    p->rem : p->punit_size;
                tprintf("%lu,%d: thread %d given chunk %d (msg %p)"
                        "tid counts (%d, %d, %d) rem %lu-%lu\n",
                        lp->gid, qi->op_id, id->tid, 
                        p->thread_chunk_id_curr, m,
                        p->nthreads_init,
                        p->nthreads_alloc_waiting, p->nthreads_fin,
                        p->rem, chunk_sz);
                t->chunk_id = p->thread_chunk_id_curr++;
                t->chunk_size = chunk_sz;
                lprintf("%lu: async-compl rem:%lu-%lu\n", lp->gid,
                        p->rem, chunk_sz);
                p->rem -= chunk_sz;

                // setup, send message
                triton_rosd_msg m_recv;
                msg_set_header(rosd_magic, RECV_CHUNK, cli_lp, &m_recv.h);
                m_recv.u.recv_chunk.id = *id;
                // control message gets high priority
                int prio = 0;
                model_net_set_msg_param(MN_MSG_PARAM_SCHED, MN_SCHED_PARAM_PRIO,
                        (void*) &prio);
                model_net_pull_event(mn_id, "rosd", cli_lp, chunk_sz,
                        0.0, sizeof(triton_rosd_msg), &m_recv, lp);
                lprintf("%lu: pull req to %lu\n", lp->gid, cli_lp);
            }
        }
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
    rosd_qitem *qi = NULL;
    rosd_callback_id id = m->u.complete_chunk_send.id;
    qlist_for_each(ent, &ns->pending_ops){
        qi = qlist_entry(ent, rosd_qitem, ql);
        if (qi->op_id == id.op_id){
            break;
        }
    }
    if (ent == &ns->pending_ops){
        int written = sprintf(ns->output_buf,
                "ERROR: pipeline op with id %d not found on LP %lu "
                "(complete_chunk_send)",
                id.op_id, lp->gid);
        lp_io_write(lp->gid, "errors", written, ns->output_buf);
        ns->error_ct = 1;
        return;
    }

    rosd_pipelined_req *p = qi->preq;
    rosd_pipelined_thread *t = &p->threads[id.tid];
    // in either case, set chunk info for rc
    m->u.complete_chunk_send.rc.chunk_id = t->chunk_id;
    m->u.complete_chunk_send.rc.chunk_size = t->chunk_size;

    p->forwarded += t->chunk_size;

    // three cases to consider:
    // - thread can read more
    // - no more work to do, but there are pending chunks to be read / sent
    // - this thread is last to send to client - ack to client

    // no more work
    if (p->rem == 0){
        // "finalize" this thread
        b->c0 = 1;
        tprintf("%lu,%d: thread %d finished (msg %p) "
                "tid counts (%d, %d, %d+1) (rem:0)\n",
                lp->gid, qi->op_id, id.tid, m, p->nthreads_init,
                p->nthreads_alloc_waiting, p->nthreads_fin);
        p->nthreads_fin++;
        resource_lp_free(t->punit_size, lp);
        // if we are the last thread to send data to the client then ack
        if (p->forwarded == qi->req.xfer_size) {
            b->c2 = 1;
            codes_store_send_resp(0, &qi->cli_cb, lp);
        }
        // if we are the last thread then cleanup req and ack to client
        if (p->nthreads_fin == p->nthreads) {
            b->c1 = 1;
            ns->bytes_read_local += p->committed;
            lprintf("%lu: rm op %d (compl chunk send)\n", lp->gid, qi->op_id);
            qlist_del(&qi->ql);
            rc_stack_push(lp, qi, free_qitem, ns->finished_ops);
        }
        // else other threads still running, do nothing
    }
    else { // more work to do
        // compute new chunk size
        uint64_t chunk_sz = p->punit_size > p->rem ?
            p->rem : p->punit_size;
        tprintf("%lu,%d: thread %d given chunk %d (msg %p)"
                "tid counts (%d, %d, %d) rem %lu-%lu\n",
                lp->gid, qi->op_id, id.tid,
                p->thread_chunk_id_curr, m,
                p->nthreads_init,
                p->nthreads_alloc_waiting, p->nthreads_fin,
                p->rem, chunk_sz);
        t->chunk_id = p->thread_chunk_id_curr++;
        t->chunk_size = chunk_sz;
        lprintf("%lu: compl send rem:%lu-%lu\n", lp->gid, p->rem, chunk_sz);
        p->rem -= chunk_sz;

        // do a read
        tw_event *e = lsm_event_new(
                ROSD_LP_NM,
                lp->gid,
                qi->req.oid,
                p->punit_size * t->chunk_id + qi->req.xfer_offset,
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

    int op_id_prev = m->u.creq.rc.op_id;

    // find the queue item (removal from list can be from any location, rc
    // pushes it back to the back, so we have to iterate)
    struct qlist_head *qitem_ent;
    rosd_qitem *qi = NULL;
    qlist_for_each(qitem_ent, &ns->pending_ops){
        qi = qlist_entry(qitem_ent, rosd_qitem, ql);
        if (qi->op_id == op_id_prev){
            break;
        }
    }
    assert(qitem_ent != &ns->pending_ops);

    lprintf("%lu: new req rc id:%d from %lu\n", lp->gid, qi->op_id,
            qi->cli_cb.h.src);

    if (m->u.creq.req.type == CSREQ_OPEN) {
        lsm_event_new_reverse(lp);
        return;
    }
    else {
        // before doing cleanups (free(...)), reverse the alloc event
        pipeline_alloc_event_rc(b, lp, op_id_prev, qi->preq);
        rosd_pipeline_destroy(qi->preq);
    }

    qlist_del(qitem_ent);
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
    rosd_qitem *qi = NULL;
    if (b->c3){
        qi = rc_stack_pop(ns->finished_ops);
        // add back into the queue
        qlist_add_tail(&qi->ql, &ns->pending_ops);
        // undo the stats
        if (qi->req.type == CSREQ_WRITE) {
            ns->bytes_written_local -= qi->preq->committed;
        }
        else {
            ns->bytes_read_local -= qi->preq->committed;
        }
        lprintf("%lu: add req %d (alloc_callback rc)\n", lp->gid, qi->op_id);
    }
    else{
        struct qlist_head *ent = NULL;
        qlist_for_each(ent, &ns->pending_ops){
            qi = qlist_entry(ent, rosd_qitem, ql);
            if (qi->op_id == m->u.palloc_callback.id.op_id){
                break;
            }
        }
        assert(ent != &ns->pending_ops);
    }
    
    int tid = m->u.palloc_callback.id.tid;

    rosd_pipelined_req *p = qi->preq;
    if (b->c0){
        p->thread_chunk_id_curr--;
        // the size computed is stored in the thread's chunk_size
        uint64_t sz = p->threads[tid].chunk_size;
        lprintf("%lu: alloc-cb rc rem:%lu+%lu\n", lp->gid, p->rem, sz);
        p->rem += sz;
        p->threads[tid].chunk_size = 0;
        p->threads[tid].chunk_id = -1;

        if (qi->req.type == CSREQ_WRITE)
            model_net_pull_event_rc(mn_id, lp);
        else if (qi->req.type == CSREQ_READ)
            lsm_event_new_reverse(lp);
        else { assert(0); }

        tprintf("%lu,%d: thread %d alloc'd before compl with chunk %d rc, "
                "tid counts (%d-1, %d+1, %d)\n",
                lp->gid, qi->op_id, tid, p->thread_chunk_id_curr,
                p->nthreads_init, p->nthreads_alloc_waiting,
                p->nthreads_fin);
        if (b->c1){
            p->nthreads_init = m->u.palloc_callback.rc.nthreads_init;
            p->nthreads_fin  = m->u.palloc_callback.rc.nthreads_fin;
        }
        else if (b->c2){
            pipeline_alloc_event_rc(b, lp, qi->op_id, p);
        }
    }
    else{
        resource_lp_free_rc(lp);
        tprintf("%lu,%d: thread %d alloc'd after compl rc, "
                "tid counts (%d-%d, %d+1, %d-%d)\n",
                lp->gid, qi->op_id, tid, p->nthreads_init,
                p->nthreads_init - m->u.palloc_callback.rc.nthreads_init,
                p->nthreads_alloc_waiting, p->nthreads_fin,
                p->nthreads_fin - m->u.palloc_callback.rc.nthreads_fin);
        p->nthreads_init = m->u.palloc_callback.rc.nthreads_init;
        p->nthreads_fin  = m->u.palloc_callback.rc.nthreads_fin;
        // note: undid the queue deletion at beginning of RC
    }
    p->nthreads_alloc_waiting++;
}
void handle_recv_chunk_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    // first look up pipeline request
    struct qlist_head *ent = NULL;
    rosd_qitem *qi = NULL; 
    qlist_for_each(ent, &ns->pending_ops){
        qi = qlist_entry(ent, rosd_qitem, ql);
        if (qi->op_id == m->u.recv_chunk.id.op_id){
            break;
        }
    }
    assert(ent != &ns->pending_ops);

    lsm_event_new_reverse(lp);

    int tid = m->u.recv_chunk.id.tid;
    rosd_pipelined_thread *t = &qi->preq->threads[tid];
    qi->preq->received -= t->chunk_size;
}

static void handle_complete_disk_op_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp){
    rosd_callback_id *id;
    int prev_chunk_id;
    uint64_t prev_chunk_size;
    id = &m->u.complete_sto.id;

    // get the operation
    rosd_qitem *qi = NULL;
    if (b->c1 || b->c3){
        // request was "deleted", put back into play
        qi = rc_stack_pop(ns->finished_ops);
        lprintf("%lu: add op %d (async compl %s rc)\n", lp->gid, qi->op_id,
                m->h.event_type==COMPLETE_DISK_OP ? "disk" : "fwd");
        qlist_add_tail(&qi->ql, &ns->pending_ops);
        // undo the stats
        if (m->u.complete_sto.is_data_op)
            ns->bytes_written_local -= qi->preq->committed;
    }
    else{
        struct qlist_head *ent = NULL;
        qlist_for_each(ent, &ns->pending_ops){
            qi = qlist_entry(ent, rosd_qitem, ql);
            if (qi->op_id == id->op_id){
                break;
            }
        }
        assert(ent != &ns->pending_ops);
    }

    if (!m->u.complete_sto.is_data_op) {
        codes_store_send_resp_rc(lp);
    }
    else {
        rosd_pipelined_req *p = qi->preq;
        rosd_pipelined_thread *t = &p->threads[id->tid];

        // set the chunk rc parameters
        if (!b->c2){ //chunk size wasn't overwritten, grab it from the thread
            prev_chunk_size = t->chunk_size;
            prev_chunk_id = -1; // unused
        }
        else {
            prev_chunk_size = m->u.complete_sto.rc.chunk_size;
            prev_chunk_id   = m->u.complete_sto.rc.chunk_id;
        }

        lprintf("%lu: commit:%lu-%lu\n", lp->gid,
                p->committed, prev_chunk_size);
        p->committed -= prev_chunk_size;

        if (qi->req.type == CSREQ_READ){
            model_net_event_rc(mn_id, lp, prev_chunk_size);
        }
        // else write && chunk op is complete
        else {
            if (b->c0){
                codes_store_send_resp_rc(lp);
            }
            // thread pulled more data
            if (b->c2){
                // note - previous derived chunk size is currently held in thread
                tprintf("%lu,%d: thread %d given chunk rc %d (msg %p), "
                        "tid counts (%d, %d, %d), rem %lu+%lu\n",
                        lp->gid, qi->op_id, id->tid, t->chunk_id,
                        m,
                        p->nthreads_init,
                        p->nthreads_alloc_waiting, 
                        p->nthreads_fin,
                        p->rem, t->chunk_size);
                lprintf("%lu: async-compl rc rem:%lu+%lu\n", lp->gid,
                        p->rem, t->chunk_size);
                p->rem += t->chunk_size;
                p->thread_chunk_id_curr--;
                t->chunk_size = prev_chunk_size;
                t->chunk_id   = prev_chunk_id;

                // we 'un-cleared' earlier, so nothing to do here

                model_net_pull_event_rc(mn_id, lp);
            }
            else{
                tprintf("%lu,%d: thread %d finished rc (msg %p), "
                        "tid counts (%d, %d, %d-1)\n",
                        lp->gid, qi->op_id, id->tid, m, p->nthreads_init,
                        p->nthreads_alloc_waiting, p->nthreads_fin);
                p->nthreads_fin--;
                resource_lp_free_rc(lp);
                // reversal of deletion occurred earlier
            }
        }
    }
}

void handle_complete_chunk_send_rc(
        triton_rosd_state * ns,
        tw_bf *b,
        triton_rosd_msg * m,
        tw_lp * lp) {
    rosd_callback_id id = m->u.complete_chunk_send.id;

    // get the operation
    rosd_qitem *qi = NULL;

    if (b->c1) {
        // request was "deleted", put back into play
        qi = rc_stack_pop(ns->finished_ops);
        lprintf("%lu: add op %d (compl chunk send rc)\n", lp->gid, qi->op_id);
        qlist_add_tail(&qi->ql, &ns->pending_ops);
        // undo the stats
        ns->bytes_read_local -= qi->preq->committed;
    }
    else{
        struct qlist_head *ent = NULL;
        qlist_for_each(ent, &ns->pending_ops){
            qi = qlist_entry(ent, rosd_qitem, ql);
            if (qi->op_id == id.op_id){
                break;
            }
        }
        assert(ent != &ns->pending_ops);
    }

    rosd_pipelined_req *p = qi->preq;
    rosd_pipelined_thread *t = &p->threads[id.tid];

    if (b->c0) {
        p->nthreads_fin--;
        resource_lp_free_rc(lp);
        if (b->c2)
            codes_store_send_resp_rc(lp);
    }
    else {
        p->rem += t->chunk_size;
        p->thread_chunk_id_curr--;
        lsm_event_new_reverse(lp);
    }

    t->chunk_id = m->u.complete_chunk_send.rc.chunk_id;
    t->chunk_size = m->u.complete_chunk_send.rc.chunk_size;
    p->forwarded -= t->chunk_size;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
