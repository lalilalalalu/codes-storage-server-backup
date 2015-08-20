/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */


#include <codes/codes_mapping.h>
#include <codes/lp-type-lookup.h>
#include <codes/jenkins-hash.h>
#include <codes/codes-workload.h>
#include <codes/codes.h>
#include <codes/quicklist.h>
#include <codes/model-net.h>
#include <codes/codes-store-lp.h>
#include <codes/codes-mapping-context.h>
#include <codes/codes-callback.h>

#include "client.h"
#include "barrier.h"
#include "io-sim-mode.h"
#include "dist.h"

#define CLIENT_DEBUG 0
#define MAX_FILE_CT 100

char const * const CLIENT_LP_NM = "codes-store-client";

static inline double sec_to_nsec(double sec){ return sec * 1e9; }
static inline double nsec_to_sec(double nsec){ return nsec * 1e-9; }

/**** BEGIN SIMULATION DATA STRUCTURES ****/

int triton_client_magic; /* use this as sanity check on events */
static int num_clients;

/* distribution parameters */
static unsigned int stripe_factor, strip_size; 

static tw_lpid barrier_lpid;

/* for mock tests */ 
static int num_writes_mock = 0;
static int num_reads_mock  = 0;
static int mock_is_write;
static int req_size_mock;

/* for workloads with open - assume the file is shared among ALL clients */
static int is_shared_open_mode = 0;

static int cli_mn_id;

/* workload-specific parameters */
static codes_workload_config_return wkld_cfg;

static struct codes_cb_info cli_cb;

typedef struct client_req client_req;

/* hold onto, at the !!!process level!!!, the random mapping from file id to
 * OID */
typedef struct file_map {
    uint64_t file_id;
    uint64_t *oids;
    struct qlist_head ql;
} file_map;
static struct qlist_head file_map_global;

struct client_req {
    file_map map;
    uint64_t *oid_offs, *oid_lens;
    int *status;
    int status_ct;
    int op_index;
    enum codes_store_req_type type; // cache for stats gathering
    double issue_time;
    struct qlist_head ql;
};

struct triton_client_state {
    int client_idx;
    /* TODO: refactor this away from assuming mock testing 
     * once we have multiple generation types */
    int reqs_remaining;

    /* workload info */
    int wkld_id;
    int wkld_done; /* no more ops for this client */

    int error_ct;

    /* queue for tracking ops (needed if ops require multiple messages) */
    int op_status_ct;
    struct qlist_head ops;

    int op_index_current;

    /* currently, we just push completed ops into here. We need a way of doing
     * our own cleanup after GVT */
    struct qlist_head complete_ops;

    /* statistics */
    int num_files;
    uint64_t file_ids[MAX_FILE_CT];
    /* a file id can be opened multiple times,
     * may not want to delete entry on reverse */
    uint64_t file_id_opens[MAX_FILE_CT];
    uint64_t read_bytes[MAX_FILE_CT];
    uint64_t write_bytes[MAX_FILE_CT];
    double open_times[MAX_FILE_CT];
    double read_times[MAX_FILE_CT];
    double write_times[MAX_FILE_CT];
    int read_count[MAX_FILE_CT];
    int write_count[MAX_FILE_CT];
    double start_read_times[MAX_FILE_CT];
    double start_write_times[MAX_FILE_CT];
    double end_read_times[MAX_FILE_CT];
    double end_write_times[MAX_FILE_CT];

#if CLIENT_DEBUG
    int event_num;
    FILE *fdbg;
#endif
};

/**** END SIMULATION DATA STRUCTURES ****/

/**** BEGIN LP, EVENT PROCESSING FUNCTION DECLS ****/

/* ROSS LP processing functions */  
static void triton_client_lp_init(
    triton_client_state * ns,
    tw_lp * lp);
static void triton_client_event_handler(
    triton_client_state * ns,
    tw_bf * b,
    triton_client_msg * m,
    tw_lp * lp);
static void triton_client_rev_handler(
    triton_client_state * ns,
    tw_bf * b,
    triton_client_msg * m,
    tw_lp * lp);
static void triton_client_finalize(
    triton_client_state * ns,
    tw_lp * lp);

/* event type handlers */
static void handle_triton_client_kickoff(
    triton_client_state * ns,
    triton_client_msg * m,
    tw_lp * lp);
static void handle_triton_client_recv_ack(
    triton_client_state * ns,
    triton_client_msg * m,
    tw_lp * lp);
/* NOTE: generating mock requests takes entirely different path than workload
 * acks */
static void handle_triton_client_recv_ack_mock(
    triton_client_state * ns,
    triton_client_msg * m,
    tw_lp * lp);
static void handle_client_wkld_next(
    triton_client_state * ns,
    triton_client_msg * m,
    tw_lp * lp);
static void handle_triton_client_recv_ack_wkld(
    triton_client_state * ns,
    triton_client_msg * m,
    tw_lp * lp);
static void handle_triton_client_kickoff_rev(
    triton_client_state * ns,
    triton_client_msg * m,
    tw_lp * lp);
static void handle_triton_client_recv_ack_rev(
    triton_client_state * ns,
    triton_client_msg * m,
    tw_lp * lp);
static void handle_triton_client_recv_ack_rev_mock(
    triton_client_state * ns,
    triton_client_msg * m,
    tw_lp * lp);
static void handle_client_wkld_next_rev(
        triton_client_state * ns,
        triton_client_msg * m,
        tw_lp * lp);
static void handle_triton_client_recv_ack_rev_wkld(
    triton_client_state * ns,
    triton_client_msg * m,
    tw_lp * lp);

/* ROSS function pointer table for this LP */
tw_lptype triton_client_lp = {
    (init_f) triton_client_lp_init,
    (pre_run_f) NULL,
    (event_f) triton_client_event_handler,
    (revent_f) triton_client_rev_handler,
    (final_f)  triton_client_finalize, 
    (map_f) codes_mapping,
    sizeof(triton_client_state),
};

/**** END LP, EVENT PROCESSING FUNCTION DECLS ****/

/**** BEGIN UTILITY DECLS ****/

struct striped_req_id
{
    int op_index;
    int strip;
};
static int striped_req_to_tag(int op_index, int strip);
static struct striped_req_id tag_to_striped_req(int tag);

static file_map* get_random_file_id_mapping(uint64_t file_id, unsigned int stripe_factor);
static client_req* client_req_init(unsigned int stripe_factor); 
static void client_req_destroy(client_req *req);
static void enter_barrier_event(int count, int root, int rank, tw_lp *lp);
static void enter_barrier_event_rc(tw_lp *lp);

/**** BEGIN IMPLEMENTATIONS ****/

void triton_client_register(){
    lp_type_register(CLIENT_LP_NM, &triton_client_lp);
}

void triton_client_configure(int model_net_id){
    uint32_t h1=0, h2=0;

    bj_hashlittle2(CLIENT_LP_NM, strlen(CLIENT_LP_NM), &h1, &h2);
    triton_client_magic = h1+h2;

    num_clients = codes_mapping_get_lp_count(NULL, 0, CLIENT_LP_NM, NULL, 1);
    assert(num_clients>0);

    INIT_CODES_CB_INFO(&cli_cb, triton_client_msg, header, tag, ret);

    barrier_lpid = codes_mapping_get_lpid_from_relative(0, NULL, "barrier",
            NULL, 0);

    cli_mn_id = model_net_id;

    char val[MAX_NAME_LENGTH];
    int rc;
    /* read in request mode */
    rc = configuration_get_value(&config, CLIENT_LP_NM, "req_mode", NULL,
            val, MAX_NAME_LENGTH);
    assert(rc>0);
    if (strncmp(val, "mock", 8) == 0){
        int rc2;
        /* read the mock workload params */
        io_sim_cli_req_mode = REQ_MODE_MOCK;
        rc = configuration_get_value_int(&config, "mock", 
                "num_writes", NULL, &num_writes_mock);
        rc2 = configuration_get_value_int(&config, "mock",
                "num_reads", NULL, &num_reads_mock);
        if (rc != 0 && rc2 != 0) {
            tw_error(TW_LOC, "Expected mock:num_writes or mock:num_reads\n");
        }
        else if (rc == 0 && rc2 == 0) {
            tw_error(TW_LOC, "Expected mock:num_writes or mock:num_reads "
                    "(not both!)\n");
        }
        else if (rc == 0) {
            assert(num_writes_mock > 0);
            mock_is_write = 1;
        }
        else {
            assert(num_reads_mock > 0);
            mock_is_write = 0;
        }
        rc = configuration_get_value_int(&config, "mock", 
                "req_size", NULL, &req_size_mock);
        assert(rc==0 && req_size_mock > 0);
    }
    else if (strncmp(val, "workload", 8) == 0){
        io_sim_cli_req_mode = REQ_MODE_WKLD;
        wkld_cfg = codes_workload_read_config(&config, "workload");
    }
    else{
        fprintf(stderr, 
                "unknown mode for triton_client:req_mode config entry\n");
        abort();
    }
    /* read OID mapping mode */
    rc = configuration_get_value(&config, CLIENT_LP_NM, "map_mode", NULL,
            val, MAX_NAME_LENGTH);
    assert(rc>0);
    if (strncmp(val, "zero", 4) == 0){
        io_sim_cli_oid_map_mode = MAP_MODE_ZERO;
    }
    else if (strncmp(val, "random_persist", 12) == 0){
        io_sim_cli_oid_map_mode = MAP_MODE_RANDOM_PERSIST;
    }
    else if (strncmp(val, "random", 6) == 0){
        io_sim_cli_oid_map_mode = MAP_MODE_RANDOM;
    }
    else if (strncmp(val, "file_id_hash", 12) == 0){
        io_sim_cli_oid_map_mode = MAP_MODE_WKLD_HASH;
    }
    else if (strncmp(val, "file_id", 7) == 0){
        io_sim_cli_oid_map_mode = MAP_MODE_WKLD_FILE_ID;
    }
    else {
        fprintf(stderr, 
                "unknown mode for triton_client:map_mode config entry\n");
        abort();
    }
    /* read striping mode */
    rc = configuration_get_value(&config, CLIENT_LP_NM, "stripe_mode", NULL,
            val, MAX_NAME_LENGTH);
    assert(rc>0);
    if (strncmp(val, "single", 6) == 0){
        io_sim_cli_dist_mode = DIST_MODE_SINGLE;
        stripe_factor = 1;
    }
    else if (strncmp(val, "rr", 2) == 0){
        io_sim_cli_dist_mode = DIST_MODE_RR;
        /* read additional parameters currently colocated with the triton 
         * client */
        rc = configuration_get_value_uint(&config, CLIENT_LP_NM, 
                "stripe_factor_rr", NULL, &stripe_factor);
        assert(rc==0);
        rc = configuration_get_value_uint(&config, CLIENT_LP_NM, 
                "strip_size_rr", NULL, &strip_size);
        assert(rc==0);
    }
    else{
        fprintf(stderr, 
                "unknown mode for triton_client:stripe_mode config entry\n");
        abort();
    }

    /* read file open mode (don't care about value, existence -> yes)
     * optional - absence -> no */
    rc = configuration_get_value_int(&config, CLIENT_LP_NM,
            "shared_object_mode", NULL, &is_shared_open_mode);

    /* initialize file_id->oid mapping */
    if (io_sim_cli_oid_map_mode == MAP_MODE_RANDOM_PERSIST){
       INIT_QLIST_HEAD(&file_map_global); 
    }
}

void triton_client_lp_init(
        triton_client_state * ns,
        tw_lp * lp){

    ns->reqs_remaining = mock_is_write ? num_writes_mock : num_reads_mock;
    ns->client_idx = codes_mapping_get_lp_relative_id(lp->gid, 0, 0);
    ns->error_ct = 0;
#if CLIENT_DEBUG
    ns->event_num = 0;
    char val[64];
    sprintf(val, "client.log.%d", ns->client_idx);
    ns->fdbg = fopen(val, "w");
    setvbuf(ns->fdbg, NULL, _IONBF, 0);
    assert(ns->fdbg);
#endif

    /* initialize workload if in workload mode */
    if (io_sim_cli_req_mode == REQ_MODE_WKLD){
        ns->wkld_done = 0;
        ns->wkld_id = codes_workload_load(wkld_cfg.type, wkld_cfg.params, 0, ns->client_idx);
        if (ns->wkld_id == -1){
            tw_error(TW_LOC, "client %d (LP %lu) unable to load workload\n",
                    ns->client_idx, lp->gid);
        }
    }
    else{
        ns->wkld_id = -1;
    }

    ns->op_status_ct = 0;
    ns->op_index_current = 0;
    INIT_QLIST_HEAD(&ns->ops);
    INIT_QLIST_HEAD(&ns->complete_ops);

    ns->num_files = 0;
    memset(ns->file_id_opens, 0, MAX_FILE_CT*sizeof(*ns->file_id_opens));
    memset(ns->read_count, 0, MAX_FILE_CT*sizeof(*ns->read_count));
    memset(ns->write_count, 0, MAX_FILE_CT*sizeof(*ns->write_count));
    int i;
    for (i = 0; i < MAX_FILE_CT; i++){
        ns->open_times[i]=0.0;
        ns->read_times[i]=0.0;
        ns->write_times[i]=0.0;
    }

    tw_event *e = codes_event_new(lp->gid, codes_local_latency(lp), lp);
    triton_client_msg *m = tw_event_data(e);
    msg_set_header(triton_client_magic, TRITON_CLI_KICKOFF, lp->gid, &m->header);
    tw_event_send(e);
}

void triton_client_event_handler(
        triton_client_state * ns,
        tw_bf * b,
        triton_client_msg * m,
        tw_lp * lp){
    assert(m->header.magic == triton_client_magic);
    
#if CLIENT_DEBUG
    fprintf(ns->fdbg, "event num %d\n", ns->event_num);
#endif
    if (ns->error_ct > 0){
        ns->error_ct++;
        return;
    }

    switch (m->header.event_type){
        case TRITON_CLI_KICKOFF:
            handle_triton_client_kickoff(ns, m, lp);
            break;
        case TRITON_CLI_RECV_ACK:
            handle_triton_client_recv_ack(ns, m, lp);
            break;
        case TRITON_CLI_WKLD_CONTINUE:
            /* proceed immediately to workload processing */
#if CLIENT_DEBUG
            fprintf(ns->fdbg, "lp %lu got continue\n", lp->gid);
#endif
            ns->op_status_ct--;
            handle_client_wkld_next(ns, m, lp);
            break;
        default:
            assert(!"triton_client event type not known");
            break;
    }
#if CLIENT_DEBUG
    ns->event_num++;
#endif
}

void triton_client_rev_handler(
        triton_client_state * ns,
        tw_bf * b,
        triton_client_msg * m,
        tw_lp * lp){
    assert(m->header.magic == triton_client_magic);

    if (ns->error_ct > 0){
        ns->error_ct--;
        if (ns->error_ct==0){
            lp_io_write_rev(lp->gid, "errors");
#if CLIENT_DEBUG
            fprintf(ns->fdbg, "left bad state through reverse\n");
#endif
        }
        return;
    }

    switch (m->header.event_type){
        case TRITON_CLI_KICKOFF:
            handle_triton_client_kickoff_rev(ns, m, lp);
            break;
        case TRITON_CLI_RECV_ACK:
            handle_triton_client_recv_ack_rev(ns, m, lp);
            break;
        case TRITON_CLI_WKLD_CONTINUE:
            /* proceed immediately to workload processing */
            handle_client_wkld_next_rev(ns, m, lp);
            break;
        default:
            assert(!"triton_client event type not known");
            break;
    }
}

void triton_client_finalize(
        triton_client_state * ns,
        tw_lp * lp){
    int i, written = 0;
    char *buf = malloc(1<<20);
    if (ns->client_idx == 0){
        written = sprintf(buf, "# Format\n"
                "# <cli>, <file id>, <total read>, <total write>,\n"
                "# times (s): <total read>, <total write>, <total open>,\n" 
                "#            <start read>, <end read>, <start write> <end write>\n");
    }
    for (i = 0; i < ns->num_files; i++){
        double read_start = 0.0, read_end = 0.0, write_start = 0.0, write_end = 0.0;
        uint64_t read_bytes = 0, write_bytes = 0;
        if (ns->read_count[i] > 0){
            read_start = nsec_to_sec(ns->start_read_times[i]);
            read_end = nsec_to_sec(ns->end_read_times[i]);
        }
        if (ns->write_count[i] > 0){
            write_start = nsec_to_sec(ns->start_write_times[i]);
            write_end = nsec_to_sec(ns->end_write_times[i]);
        }
        if (ns->file_id_opens[i] > 0){
            read_bytes = ns->read_bytes[i];
            write_bytes = ns->write_bytes[i];
        }
        written += sprintf(buf+written, 
                "%d %20lu %14lu %14lu %1.5e %1.5e %1.5e %1.5e %1.5e %1.5e %1.5e\n",
                ns->client_idx, ns->file_ids[i], read_bytes, 
                write_bytes, nsec_to_sec(ns->read_times[i]), 
                nsec_to_sec(ns->write_times[i]), nsec_to_sec(ns->open_times[i]),
                read_start, read_end, write_start, write_end);
    }
    if (written > 0){
        lp_io_write(lp->gid, "client_rw", written, buf);
    }
    free(buf);
#if CLIENT_DEBUG
    fclose(ns->fdbg);
#endif
    /* error checking: client shouldn't have outstanding requests */
    if (ns->op_status_ct > 0){
        fprintf(stderr, 
                "WARNING: LP %lu, client %d: %d pending operations "
                "incomplete at finalize\n", 
                lp->gid, ns->client_idx,ns->op_status_ct);
    }
}

/* event type handlers */
void handle_triton_client_kickoff(
        triton_client_state * ns,
        triton_client_msg * m,
        tw_lp * lp){
    switch(io_sim_cli_req_mode){
        case REQ_MODE_MOCK:
            /* cheat: inc remaining requests and process a fake success ack
             * - acks events generate new requests */
            m->ret = CODES_STORE_OK;
            ns->reqs_remaining++;
            ns->op_status_ct++;
            handle_triton_client_recv_ack_mock(ns,m,lp);
            break;
        case REQ_MODE_WKLD:
            handle_client_wkld_next(ns,m,lp);
            break;
        default:
            tw_error(TW_LOC, "unexpected or uninitialized request mode");
    }
}

void handle_triton_client_recv_ack_mock(
        triton_client_state * ns,
        triton_client_msg * m,
        tw_lp * lp){

    /* decrement on receipt rather than on send */
    ns->reqs_remaining--;
    ns->op_status_ct--;

    if(ns->reqs_remaining > 0){
        /* compute dest oid and server */
        uint64_t oid;
        switch (io_sim_cli_oid_map_mode){
            case MAP_MODE_RANDOM:
                /* TODO: proper oid RNG (ROSS boils down to an fp) */
                oid = (uint64_t)(tw_rand_unif(lp->rng)*(double)UINT64_MAX);
                break;
            case MAP_MODE_ZERO:
                oid = 0;
                break;
            default:
                assert(!"unexpected or unitialized mapping mode");
        }

        msg_header h;
        struct codes_store_request r;
        msg_set_header(triton_client_magic, TRITON_CLI_RECV_ACK, lp->gid, &h);
        codes_store_init_req(mock_is_write ? CSREQ_WRITE : CSREQ_READ,
                0, oid, 0, req_size_mock, &r);

        codes_store_send_req(&r, 0, lp, cli_mn_id, CODES_MCTX_DEFAULT, 0, &h,
                &cli_cb);

        ns->op_status_ct ++;

#if CLIENT_DEBUG && 0
        fprintf(ns->fdbg, "LP %lu sending req for oid %lu to srv %lu\n", lp->gid, oid, srv_lp);
#endif
    }
}

void handle_client_wkld_next(
        triton_client_state * ns,
        triton_client_msg * m,
        tw_lp * lp){
    /* process the next request */
    struct codes_workload_op op;

    /* currently, we do not handle concurrent requests */
    if (ns->op_status_ct > 0){
        char buf[64];
        int written = sprintf(buf, 
                "client %d: request already in flight\n",
                ns->client_idx);
        lp_io_write(lp->gid, "errors", written, buf);
        ns->error_ct = 1;
        return;
    }

    codes_workload_get_next(ns->wkld_id, 0, ns->client_idx, &op);
    /* cache the op for reverse processing */
    m->op = op;

#if CLIENT_DEBUG
    codes_workload_print_op(ns->fdbg, &op, 0, ns->client_idx);
#endif 
    if (op.op_type == CODES_WK_END){
        /* workload finished */
        ns->wkld_done = 1;
        return;
    }

    /* all other ops will get an "ack" of one kind or another */
    ns->op_status_ct++;
    if (op.op_type == CODES_WK_OPEN || op.op_type == CODES_WK_READ ||
             op.op_type == CODES_WK_WRITE){
        int is_open = op.op_type == CODES_WK_OPEN;
        int is_write= op.op_type == CODES_WK_WRITE;
        /* operations that create an ROSD request across the network */

        msg_header h;
        struct codes_store_request r;
        msg_set_header(triton_client_magic, TRITON_CLI_RECV_ACK, lp->gid, &h);
        m->op_index_prev = ns->op_index_current;

        uint64_t file_id, off, len;
        if (is_open){
            r.type = op.u.open.create_flag ? CSREQ_CREATE : CSREQ_OPEN;
            file_id = op.u.open.file_id;
            len = 1;
            /* NON-ROOT (rank 0) processes - if in shared file mode go
             * immediately into the barrier */
            if (is_shared_open_mode && ns->client_idx != 0) {
                /* current implementation - contact the sentinel barrier LP
                 * directly, without going through the network */
                enter_barrier_event(0, num_clients, ns->client_idx, lp);
                /* in addition, we must ensure the file pointer exists before
                 * we process the next workload item */
                int f;
                for (f = 0; f < ns->num_files &&
                        file_id != ns->file_ids[f]; f++);
                if (f == ns->num_files) {
                    assert(ns->num_files < MAX_FILE_CT);
                    ns->num_files++;
                    ns->file_ids[f] = file_id;
                    ns->file_id_opens[f] = 1;
                    ns->read_bytes[f] = 0;
                    ns->write_bytes[f] = 0;
                }
                return;
            }
        }
        else if (is_write){
            r.type = CSREQ_WRITE;
            file_id = op.u.write.file_id;
            off = op.u.write.offset;
            len = op.u.write.size;
        }
        else{
            r.type = CSREQ_READ;
            file_id = op.u.read.file_id;
            off = op.u.read.offset;
            len = op.u.read.size;
        }
        /* find the entry for this file */
        int fpos; 
        for (fpos = 0; fpos < ns->num_files && 
                file_id != ns->file_ids[fpos]; fpos++);
        /* if it doesn't exist and we're opening, create */
        if (fpos == ns->num_files) {
            assert(is_open);
            assert(ns->num_files < MAX_FILE_CT);
            ns->num_files++;
            ns->file_ids[fpos] = file_id;
            ns->file_id_opens[fpos]++;
            ns->read_bytes[fpos] = 0;
            ns->write_bytes[fpos] = 0;
        }
        else if (is_open){
            if (ns->file_id_opens[fpos]==0){
                ns->read_bytes[fpos] = 0;
                ns->write_bytes[fpos] = 0;
            }
            ns->file_id_opens[fpos]++;
        }
        /* increment r/w counters, set start time */
        else if (is_write){
            assert(fpos != ns->num_files);
            ns->write_bytes[fpos] += len;
            if (ns->write_count[fpos] == 0){
                ns->start_write_times[fpos] = tw_now(lp);
            }
            ns->write_count[fpos]++;
        }
        else {
            assert(fpos != ns->num_files);
            ns->read_bytes[fpos] += len;
            if (ns->read_count[fpos] == 0){
                ns->start_read_times[fpos] = tw_now(lp);
            }
            ns->read_count[fpos]++;
        }

        /* initialize the queue item we'll be adding */
        client_req *req = client_req_init(stripe_factor);
        req->op_index = ns->op_index_current;
        req->issue_time = tw_now(lp);
        req->type = r.type;

        /* from here, either error or success, so go ahead and add to queue */
        qlist_add_tail(&req->ql, &ns->ops); 

        /* generate the oids. Via the stripe_factor=1 trick, this logic is the
         * same regardless of striping, but it will likely change later 
         * NOTE: for the persistent random case, we have a dedicated function */
        unsigned int i;
        if (io_sim_cli_oid_map_mode != MAP_MODE_RANDOM_PERSIST){
            req->map.file_id = file_id; 
            req->map.oids = malloc(stripe_factor*sizeof(req->map.oids)); 
            for (i = 0; i < stripe_factor; i++){
                switch(io_sim_cli_oid_map_mode){
                    case MAP_MODE_ZERO:
                        assert(i==0); /* can't be called with striping */
                        req->map.oids[i] = 0; 
                        break;
                    case MAP_MODE_WKLD_FILE_ID:
                        req->map.oids[i] = file_id + i;
                        break;
                    case MAP_MODE_WKLD_HASH: ;
                        uint64_t file_id_strip = file_id + i;
                        uint32_t h1 = 0, h2 = 0;
                        bj_hashlittle2(&file_id_strip, sizeof(uint64_t), &h1, &h2);
                        req->map.oids[i] = (((uint64_t)h1)<<32ull) | ((uint64_t)h2); 
                        break;
                    default:
                        tw_error(TW_LOC, "bad map mode for striped request workload");
                }
            }
        }
        else{
            req->map = *get_random_file_id_mapping(file_id, stripe_factor); 
        }

        int srv; unsigned long srv_l;
        if (io_sim_cli_dist_mode == DIST_MODE_SINGLE){
            /* prepare the message to a single oid */
            switch(io_sim_cli_oid_map_mode){
                case MAP_MODE_ZERO:
                    srv = 0;
                    break;
                case MAP_MODE_WKLD_FILE_ID:
                case MAP_MODE_WKLD_HASH:
                    // TODO: placement_find_closest(req->map.oids[0],1,&srv_l);
                    // TODO: srv = srv_l;
                    srv = 0;
                    break;
                case MAP_MODE_RANDOM_PERSIST:
                    req->map = *get_random_file_id_mapping(file_id, 1);
                    break;
                default:
                    tw_error(TW_LOC, "bad map mode for non-striped request");
            }
            r.xfer_offset  = off;
            r.xfer_size    = len;
            r.oid = req->map.oids[0];
            // TODO: calculate server index
            codes_store_send_req(&r, srv, lp, cli_mn_id,
                    CODES_MCTX_DEFAULT,
                    striped_req_to_tag(ns->op_index_current, 0), &h, &cli_cb);
            req->status_ct++;
            req->status[0]++;
#if CLIENT_DEBUG && TODO
            unsigned long rosd_ul = 0;
            /* TODO: placement_find_closest(mr.req.oid, 1, &rosd_ul);*/
            /* TODO: tw_lpid rosd_lp = get_rosd_lpid(rosd_ul);*/
            fprintf(ns->fdbg,
                    "LP %lu sending req for oid %lu to srv %lu\n",
                    lp->gid, req->map.oids[0], rosd_lp);
#endif
        }
        else{
            /* generate the requests */
            if (!is_open){
                map_logical_to_physical_objs(stripe_factor, strip_size, off, 
                        len, req->oid_offs, req->oid_lens);
            }
            MN_START_SEQ();
            for (i = 0; i < stripe_factor; i++){
                r.oid = req->map.oids[i];
                if (is_open){
                    req->status_ct++;
                    req->status[i]++;
                    r.xfer_size = 1;
                    // TODO: compute server id
                    codes_store_send_req(&r, 0, lp, cli_mn_id,
                            CODES_MCTX_DEFAULT,
                            striped_req_to_tag(ns->op_index_current, i),
                            &h, &cli_cb);
#if CLIENT_DEBUG
                    unsigned long rosd_ul = 0;
                    /* TODO: placement_find_closest(mr.req.oid, 1, &rosd_ul); */
                    /* TODO: tw_lpid rosd_lp = get_rosd_lpid(rosd_ul);*/
                    fprintf(ns->fdbg,
                            "LP %lu sending req for oid %lu to srv %lu\n",
                            lp->gid, req->map.oids[i], rosd_lp);
#endif
                }
                else if (req->oid_lens[i] > 0){
                    req->status_ct++;
                    req->status[i]++;
                    r.xfer_offset = req->oid_offs[i];
                    r.xfer_size   = req->oid_lens[i];
                    // TODO: compute server id
                    codes_store_send_req(&r, 0, lp, cli_mn_id,
                            CODES_MCTX_DEFAULT,
                            striped_req_to_tag(ns->op_index_current, i),
                            &h, &cli_cb);
#if CLIENT_DEBUG && TODO
                    unsigned long rosd_ul;
                    // TODO: placement_find_closest(mr.req.oid, 1, &rosd_ul);
                    // TODO: tw_lpid rosd_lp = get_rosd_lpid(rosd_ul);
                    fprintf(ns->fdbg,
                            "LP %lu sending req for oid %lu to srv %lu, size %lu\n",
                            lp->gid, req->map.oids[i], rosd_lp, mr.req.xfer_size);
#endif
                }
            }
            MN_END_SEQ();
        }
        // in either case, update the opid
        ns->op_index_current++;
    }
    else if (op.op_type == CODES_WK_DELAY || op.op_type == CODES_WK_CLOSE){
        /* operations that simply invoke another workload op */
        double delay = (op.op_type==CODES_WK_DELAY) ? 
            sec_to_nsec(op.u.delay.seconds) : 0.0;
        tw_event *e = codes_event_new(lp->gid, 
                codes_local_latency(lp)+delay, lp);
        triton_client_msg *m_next = tw_event_data(e);
        *m_next = *m;
        m_next->header.event_type = TRITON_CLI_WKLD_CONTINUE;
        tw_event_send(e);
    }
    else if (op.op_type == CODES_WK_BARRIER){
        /* current implementation - contact the sentinel barrier LP 
         * directly, without going through the network */
        /* a count of -1 means all ranks (with an assumed root of 0) */
        assert(!(op.u.barrier.count == -1 && op.u.barrier.root != 0));
        enter_barrier_event(op.u.barrier.root,
                (op.u.barrier.count == -1) ? num_clients : op.u.barrier.count,
                ns->client_idx, lp);
    }
    else{
        /* no-op: io kernel lang has non-codes operations
        tw_error(TW_LOC, "unknown workload operation");
        */
        tw_event *e = codes_event_new(lp->gid,
                codes_local_latency(lp), lp);
        triton_client_msg *m_next = tw_event_data(e);
        *m_next = *m;
        m_next->header.event_type = TRITON_CLI_WKLD_CONTINUE;
        tw_event_send(e);
    }
}

void handle_triton_client_recv_ack_wkld(
        triton_client_state * ns,
        triton_client_msg * m,
        tw_lp * lp){
#if CLIENT_DEBUG
    fprintf(ns->fdbg, "client recv ack\n");
#endif
    /* under the current workload processing model, we explicitly process an
     * ack event corresponding to the done state. After that point, we should no
     * longer recieve any acks */
    if (ns->wkld_done){
        char buf[128];
        int written = sprintf(buf, "client %d, recv ack from finished wkld\n",
                ns->client_idx);
        lp_io_write(lp->gid, "errors", written, buf);
        ns->error_ct = 1;
#if CLIENT_DEBUG
        fprintf(ns->fdbg, "entered bad state\n");
#endif
        return;
    }
    /*assert(!ns->wkld_done);*/

    struct striped_req_id rid = tag_to_striped_req(m->tag);

    /* first find the associated operation */
    struct qlist_head *ent;
    client_req *req;
    qlist_for_each(ent, &ns->ops){
        client_req *tmp = qlist_entry(ent,client_req,ql);
        if (tmp->op_index == rid.op_index){
            req = tmp;
            break;
        }
    }
    /* we better have found it... */
    if (ent == &ns->ops){
        char buf[128];
        int written = sprintf(buf,
                "client %d: recv unexpected ack from LP %lu, "
                "op idx %d, strip %d (tag %d)\n",
                ns->client_idx, m->header.src,
                rid.op_index, rid.strip, m->tag);
        lp_io_write(lp->gid, "errors", written, buf);
        ns->error_ct = 1;
#if CLIENT_DEBUG
        fprintf(ns->fdbg, "entered bad state\n");
#endif
        return;
    }

    /* ensure we are expecting this ack - no duplicates */
    if (req->status_ct == 0 || req->status[rid.strip] == 0){
        char buf[128];
        int written = sprintf(buf, "client %d recv'd unexpected ack\n",
                ns->client_idx);
        lp_io_write(lp->gid, "errors", written, buf);
        ns->error_ct = 1;
#if CLIENT_DEBUG
        fprintf(ns->fdbg, "entered bad state\n");
#endif
        return;
    }
    else{
        req->status_ct--;
        req->status[rid.strip]--;
    }

    if (req->status_ct == 0){
        /* update time */
        double diff = tw_now(lp) - req->issue_time;
#if CLIENT_DEBUG
        printf("CLIENT %d (lp %lu): op time %1.5e at %1.5e\n",
                ns->client_idx, lp->gid, diff, tw_now(lp));
#endif
        // find the corresponding stats
        int f;
        for (f = 0; f < ns->num_files && 
                ns->file_ids[f] != req->map.file_id; f++);
        assert(f < ns->num_files);
        if (req->type == CSREQ_OPEN){
            m->prev_time = ns->open_times[f];
            ns->open_times[f] += diff;
        }
        else if (req->type == CSREQ_WRITE){
            m->prev_time = ns->write_times[f];
            ns->write_times[f] += diff;
            // update end time regardless
            ns->end_write_times[f] = tw_now(lp);
        }
        else{
            m->prev_time = ns->read_times[f];
            ns->read_times[f] += diff;
            // update end time regardless
            ns->end_read_times[f] = tw_now(lp);
        }
        //tw_output(lp,"%d times: prev:%.17e now:%.17e issue:%.17e, event:%5d\n", 
                //ns->client_idx, m->prev_time, tw_now(lp), req->issue_time, 
                //ns->event_num);


        ns->op_status_ct--;
        if (is_shared_open_mode && req->type == CSREQ_OPEN &&
                ns->client_idx == 0) {
            // current implementation - contact the sentinel barrier LP
            // directly, without going through the network
            enter_barrier_event(0, num_clients, ns->client_idx, lp);
            return;
        }
        else
            handle_client_wkld_next(ns, m, lp);
        /* move the entry to the completed q */
        qlist_del(ent);
        qlist_add_tail(ent, &ns->complete_ops);
    }
}

void handle_triton_client_recv_ack(
        triton_client_state * ns,
        triton_client_msg * m,
        tw_lp * lp){

    assert(m->ret == CODES_STORE_OK);

    switch(io_sim_cli_req_mode){
        case REQ_MODE_MOCK:
            handle_triton_client_recv_ack_mock(ns,m,lp);
            break;
        case REQ_MODE_WKLD:
            handle_triton_client_recv_ack_wkld(ns,m,lp);
            break;
        default:
            assert(!"unexpected or uninitialized client request mode");
    }
}

void handle_triton_client_kickoff_rev(
        triton_client_state * ns,
        triton_client_msg * m,
        tw_lp * lp){
    /* anti-cheat ;) see forward handler */
    switch(io_sim_cli_req_mode){
        case REQ_MODE_MOCK:
            ns->reqs_remaining--;
            handle_triton_client_recv_ack_rev_mock(ns,m,lp);
            break;
        case REQ_MODE_WKLD:
            handle_client_wkld_next_rev(ns,m,lp);
            break;
        default:
            tw_error(TW_LOC, "unexpected or uninitialized request mode");
    }
}

void handle_triton_client_recv_ack_rev_mock(
        triton_client_state * ns,
        triton_client_msg * m,
        tw_lp * lp){
    ns->reqs_remaining++;
    codes_store_send_req_rc(cli_mn_id, lp);
    if (io_sim_cli_oid_map_mode == MAP_MODE_RANDOM){
        tw_rand_reverse_unif(lp->rng);
    }
}

void handle_client_wkld_next_rev(
        triton_client_state * ns,
        triton_client_msg * m,
        tw_lp * lp){
    codes_workload_get_next_rc(ns->wkld_id, 0, ns->client_idx, &m->op);
#if CLIENT_DEBUG
    fprintf(ns->fdbg, "REVERSE:\n");
    codes_workload_print_op(ns->fdbg, &m->op, 0, ns->client_idx);
#endif
    enum codes_workload_op_type type = m->op.op_type;
    if (type == CODES_WK_END){
        ns->wkld_done = 0;
        return;
    }

    ns->op_status_ct--;
    if (type == CODES_WK_OPEN || type == CODES_WK_READ ||
             type == CODES_WK_WRITE){
        int is_open  = type == CODES_WK_OPEN;
        int is_write = type == CODES_WK_WRITE;
        /* first find the operation, which must be in the pending ops at this
         * point */
        struct qlist_head *ent;
        client_req *req;
        qlist_for_each(ent, &ns->ops){
            client_req *tmp = qlist_entry(ent,client_req,ql);
            if (tmp->op_index == m->op_index_prev){
                req = tmp;
                break;
            }
        }
        assert(ent != &ns->ops);

        uint64_t file_id, len;
        if (is_open) { 
            file_id = m->op.u.open.file_id; 
            if (is_shared_open_mode && ns->client_idx != 0) {
                enter_barrier_event_rc(lp);
                /* undo the file stat addition */
                int f;
                for (f = 0; f < ns->num_files &&
                        file_id != ns->file_ids[f]; f++);
                if (f != ns->num_files) {
                    ns->num_files--;
                }
                return;
            }
        }
        else if (is_write) { 
            file_id = m->op.u.write.file_id; 
            len = m->op.u.write.size;
        }
        else { 
            file_id = m->op.u.read.file_id; 
            len = m->op.u.read.size;
        }
        /* reverse the stats */
        int fpos;
        for (fpos = 0; fpos < ns->num_files && 
                ns->file_ids[fpos] != file_id; fpos++);
        assert(fpos != ns->num_files);
        if (is_open){
            if (fpos == ns->num_files-1 && ns->file_id_opens[fpos] == 1){
                ns->num_files--;
            }
            else{
                ns->file_id_opens[fpos]--;
            }
        }
        else if (is_write){
            ns->write_bytes[fpos] -= len;
            ns->write_count[fpos]--;
        }
        else{
            ns->read_bytes[fpos] -= len;
            ns->read_count[fpos]--;
        }

        if (io_sim_cli_dist_mode == DIST_MODE_SINGLE){
            codes_store_send_req_rc(cli_mn_id, lp);
        }
        else {
            for (int i = 0; i < stripe_factor; i++) {
                if (is_open || req->oid_lens[i] > 0)
                    codes_store_send_req_rc(cli_mn_id, lp);
            }
        }

        /* remove and destroy the newly added operation */
        qlist_del(ent);
        client_req_destroy(req);
    }
    else if (type == CODES_WK_DELAY || type == CODES_WK_CLOSE) {
        codes_local_latency_reverse(lp);
    }
    else if (type == CODES_WK_BARRIER){
        enter_barrier_event_rc(lp);
    }
    else{
        /* no-op:
         * tw_error(TW_LOC, "unkown workload operation");*/
        codes_local_latency_reverse(lp);
    }
}
static void handle_triton_client_recv_ack_rev_wkld(
        triton_client_state * ns,
        triton_client_msg * m,
        tw_lp * lp){

    struct striped_req_id rid = tag_to_striped_req(m->tag);

    /* find the operation! */
    struct qlist_head *ent;
    client_req *req;
    qlist_for_each(ent, &ns->ops){
        client_req *tmp = qlist_entry(ent,client_req,ql);
        if (tmp->op_index == rid.op_index){
            req = tmp;
            break;
        }
    }
    /* if the item does not exist in the pending ops list, then it must 
     * exist in the completed ops list */
    if (ent == &ns->ops){
        qlist_for_each(ent, &ns->complete_ops){
            client_req *tmp = qlist_entry(ent,client_req,ql);
            if (tmp->op_index == rid.op_index){
                req = tmp;
                break;
            }
        }
        assert(ent != &ns->complete_ops);
        /* re-add into the pending ops list */
        qlist_del(ent);
        qlist_add_tail(ent, &ns->ops);
    }

    if (rid.strip == stripe_factor)
        tw_error(TW_LOC, "client %d: ack in rc not matched\n", ns->client_idx);
    assert(req->status[rid.strip] == 0);

    if (req->status_ct == 0){
        int f;
        for (f = 0; f < ns->num_files && 
                ns->file_ids[f] != req->map.file_id; f++);
        assert(f < ns->num_files);
        if (req->type == CSREQ_OPEN){
            ns->open_times[f] = m->prev_time;
        }
        else if (req->type == CSREQ_WRITE){
            ns->write_times[f] = m->prev_time;
        }
        else{
            ns->read_times[f] = m->prev_time;
        }

        ns->op_status_ct++;
        if (is_shared_open_mode && req->type == CSREQ_OPEN &&
                ns->client_idx == 0)
            enter_barrier_event_rc(lp);
        else
            handle_client_wkld_next_rev(ns,m,lp);
    }
    req->status_ct++;
    req->status[rid.strip]++;
}

void handle_triton_client_recv_ack_rev(
        triton_client_state * ns,
        triton_client_msg * m,
        tw_lp * lp){
    switch(io_sim_cli_req_mode){
        case REQ_MODE_MOCK:
            ns->reqs_remaining--;
            handle_triton_client_recv_ack_rev_mock(ns,m,lp);
            break;
        case REQ_MODE_WKLD:
            handle_triton_client_recv_ack_rev_wkld(ns,m,lp);
            break;
        default:
            tw_error(TW_LOC, "unexpected or uninitialized client request mode");
    }
}

/**** END IMPLEMENTATIONS ****/

static int striped_req_to_tag(int op_index, int strip)
{
    return op_index * stripe_factor + strip;
}
static struct striped_req_id tag_to_striped_req(int tag)
{
    struct striped_req_id rtn;
    rtn.op_index = tag / stripe_factor;
    rtn.strip    = tag % stripe_factor;
    return rtn;
}

static file_map* get_random_file_id_mapping(
        uint64_t file_id, 
        unsigned int stripe_factor){
    /* attempt to find an already-generated file id */
    struct qlist_head *ent;
    qlist_for_each(ent, &file_map_global){
        file_map *tmp = qlist_entry(ent,file_map,ql);
        if (tmp->file_id == file_id){
            return tmp;
        }
    }
    /* failed, create a new entry */
    file_map *fm = malloc(sizeof(file_map));
    fm->oids = malloc(stripe_factor*sizeof(*fm->oids));
    fm->file_id = file_id;
    /* make a dummy striped allocation via the placement algorithm 
     * TODO: we shouldn't have to leak ROSD details (replication factor) here */
    unsigned int num_objs;
    unsigned long *sizes_dummy = malloc(stripe_factor*sizeof(*sizes_dummy));
    /* TODO: placement_create_striped(stripe_factor, replication_factor, stripe_factor,
            1, &num_objs, fm->oids, sizes_dummy); */
    assert(num_objs == stripe_factor);
    free(sizes_dummy);
    qlist_add_tail(&fm->ql, &file_map_global);
    return fm;
}

client_req* client_req_init(unsigned int stripe_factor){
    client_req *rtn = malloc(sizeof(client_req));
    /* allocated by get_random_file_id_mapping
    rtn->fm.oids = malloc(stripe_factor*sizeof(*rtn->fm.oids));*/
    rtn->oid_offs = calloc(stripe_factor,sizeof(*rtn->oid_offs));
    rtn->oid_lens = calloc(stripe_factor,sizeof(*rtn->oid_lens));
    rtn->status = calloc(stripe_factor,sizeof(*rtn->status));
    rtn->status_ct = 0;
    return rtn;
}

void client_req_destroy(client_req *req){
    free(req->oid_offs);
    free(req->oid_lens);
    free(req->status);
    free(req);
}

void enter_barrier_event(int root, int count, int rank, tw_lp *lp){
    tw_event *e = codes_event_new(barrier_lpid,
            codes_local_latency(lp), lp);
    barrier_msg *mb = tw_event_data(e);
    mb->magic = barrier_magic;
    mb->src = lp->gid;
    mb->count = count;
    mb->root  = root;
    mb->rank  = rank;
    tw_event_send(e);
}

void enter_barrier_event_rc(tw_lp *lp){
    codes_local_latency_reverse(lp);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
