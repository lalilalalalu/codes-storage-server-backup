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
#include <codes/codes-store-client-lp.h>
#include <codes/codes-store-client-barrier-lp.h>

#include "io-sim-mode.h"
#include "dist.h"
#include "oid-map.h"

#define CLIENT_DEBUG 0
#define MAX_FILE_CT 100

char const * const CLIENT_LP_NM = "codes-store-client";

static inline double sec_to_nsec(double sec){ return sec * 1e9; }
static inline double nsec_to_sec(double nsec){ return nsec * 1e-9; }

/**** BEGIN SIMULATION DATA STRUCTURES ****/

int cs_client_magic; /* use this as sanity check on events */

/* configuration values */
static int num_clients;
static int num_servers;
static struct io_sim_config cli_config;
static struct oid_map *oid_map_method;
// codes-store callback
static struct codes_cb_info cli_cb;
// modelnet id
static int cli_mn_id;

/* distribution parameters */

static tw_lpid barrier_lpid;

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
    file_map *map;
    uint64_t *oid_offs, *oid_lens;
    int *status;
    int status_ct;
    int op_index;
    enum codes_store_req_type type; // cache for stats gathering
    double issue_time;
    struct qlist_head ql;
};

struct cs_client_state {
    int client_idx;

    /* workload info */
    int wkld_id;
    int wkld_done; /* no more ops for this client */

    int error_ct;

    /* queue for tracking ops (needed if ops require multiple messages) */
    int op_status_ct;
    struct qlist_head ops;

    int op_index_current;

    // for PLC_MODE_LOCAL, the local server this LP will be communicating with
    int server_idx_local;

    // list of mappings of file ids to oids
    struct qlist_head file_id_mappings;

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
static void cs_client_lp_init(
    cs_client_state * ns,
    tw_lp * lp);
static void cs_client_event_handler(
    cs_client_state * ns,
    tw_bf * b,
    cs_client_msg * m,
    tw_lp * lp);
static void cs_client_rev_handler(
    cs_client_state * ns,
    tw_bf * b,
    cs_client_msg * m,
    tw_lp * lp);
static void cs_client_finalize(
    cs_client_state * ns,
    tw_lp * lp);

/* event type handlers */
static void handle_cs_client_kickoff(
    cs_client_state * ns,
    tw_bf * b,
    cs_client_msg * m,
    tw_lp * lp);
static void handle_cs_client_recv_ack(
    cs_client_state * ns,
    tw_bf *b,
    cs_client_msg * m,
    tw_lp * lp);
static void handle_client_wkld_next(
    cs_client_state * ns,
    tw_bf * b,
    cs_client_msg * m,
    tw_lp * lp);
static void handle_cs_client_recv_ack_wkld(
    cs_client_state * ns,
    tw_bf * b,
    cs_client_msg * m,
    tw_lp * lp);
static void handle_cs_client_kickoff_rev(
    cs_client_state * ns,
    tw_bf * b,
    cs_client_msg * m,
    tw_lp * lp);
static void handle_cs_client_recv_ack_rev(
    cs_client_state * ns,
    tw_bf * b,
    cs_client_msg * m,
    tw_lp * lp);
static void handle_client_wkld_next_rev(
        cs_client_state * ns,
        tw_bf * b,
        cs_client_msg * m,
        tw_lp * lp);
static void handle_cs_client_recv_ack_rev_wkld(
    cs_client_state * ns,
    tw_bf * b,
    cs_client_msg * m,
    tw_lp * lp);

/* ROSS function pointer table for this LP */
tw_lptype cs_client_lp = {
    (init_f) cs_client_lp_init,
    (pre_run_f) NULL,
    (event_f) cs_client_event_handler,
    (revent_f) cs_client_rev_handler,
    (final_f)  cs_client_finalize, 
    (map_f) codes_mapping,
    sizeof(cs_client_state),
};

/**** END LP, EVENT PROCESSING FUNCTION DECLS ****/

/**** BEGIN UTILITY DECLS ****/

struct striped_req_id
{
    int op_index;
    int strip;
};

/* wrapper for tw random calls for oid_map_create_stripe_random */
static uint64_t tw_rand_u64(void *s /* <-- tw_rng_stream* */);
static void tw_rand_u64_rc(void *s /* <-- tw_rng_stream* */);
static int striped_req_to_tag(int op_index, int strip);
static struct striped_req_id tag_to_striped_req(int tag);

static file_map* get_file_id_mapping(
        cs_client_state *ns,
        tw_bf *b,
        uint64_t file_id,
        uint64_t (*rng_fn)(void *),
        void * rng_arg,
        int *num_rng_calls);
static void get_file_id_mapping_rc(
        cs_client_state *ns,
        tw_bf *b,
        uint64_t file_id,
        void (*rng_fn_rc) (void *),
        void * rng_arg,
        int num_rng_calls);
static client_req* client_req_init(unsigned int stripe_factor); 
static void client_req_destroy(client_req *req);
static void enter_barrier_event(int count, int root, int rank, tw_lp *lp);
static void enter_barrier_event_rc(tw_lp *lp);

/**** BEGIN IMPLEMENTATIONS ****/

void cs_client_register(){
    lp_type_register(CLIENT_LP_NM, &cs_client_lp);
}

void cs_client_configure(int model_net_id){
    uint32_t h1=0, h2=0;

    bj_hashlittle2(CLIENT_LP_NM, strlen(CLIENT_LP_NM), &h1, &h2);
    cs_client_magic = h1+h2;

    // we use two RNGs - one for file id mappings, the other for everything else
    if (g_tw_nRNG_per_lp < 2)
        g_tw_nRNG_per_lp++;

    num_clients = codes_mapping_get_lp_count(NULL, 0, CLIENT_LP_NM, NULL, 1);
    assert(num_clients>0);
    num_servers =
        codes_mapping_get_lp_count(NULL, 0, CODES_STORE_LP_NAME, NULL, 1);
    assert(num_servers > 0);

    INIT_CODES_CB_INFO(&cli_cb, cs_client_msg, header, tag, ret);

    barrier_lpid = codes_mapping_get_lpid_from_relative(0, NULL, "barrier",
            NULL, 0);

    cli_mn_id = model_net_id;

    io_sim_read_config(&config, CLIENT_LP_NM, NULL, num_clients, &cli_config);

    assert(is_sim_mode_init(&cli_config) && is_valid_sim_config(&cli_config));

    /* initialize file_id->oid mapping */
    if (cli_config.oid_gen_mode == GEN_MODE_RANDOM_PERSIST){
       INIT_QLIST_HEAD(&file_map_global); 
    }
    else if (cli_config.oid_gen_mode == GEN_MODE_PLACEMENT) {
        switch(cli_config.placement_mode) {
            case PLC_MODE_UNINIT:
                assert(0);
                oid_map_method = NULL;
                break;
            case PLC_MODE_ZERO:
                oid_map_method = oid_map_create_zero();
                break;
            case PLC_MODE_BINNED:
                oid_map_method = oid_map_create_bin(num_servers);
                break;
            case PLC_MODE_MOD:
                oid_map_method = oid_map_create_mod(num_servers);
                break;
            case PLC_MODE_LOCAL:
                oid_map_method = oid_map_create_mod(num_servers);
                return;
        }
    }
}

void cs_client_lp_init(
        cs_client_state * ns,
        tw_lp * lp)
{
    ns->client_idx = codes_mapping_get_lp_relative_id(lp->gid, 0, 0);

    // initialize the RNG to the *same state*
    // TODO: configure a seed value or use an out of range lpid
    tw_rand_initial_seed(lp->rng, 0);

    ns->wkld_done = 0;
    ns->wkld_id = codes_workload_load(cli_config.req_cfg.type,
            cli_config.req_cfg.params, 0, ns->client_idx);
    if (ns->wkld_id == -1){
        tw_error(TW_LOC, "client %d (LP %lu) unable to load workload\n",
                ns->client_idx, lp->gid);
    }

    if (cli_config.placement_mode == PLC_MODE_LOCAL) {
        // TODO: do we need finer-grained control over the mapping? This one
        // just does wraparound assignment
        char const * group = NULL;
        int rep = 0, offset = 0;
        int servers_per_group = 0;
        tw_lpid cs_id = -1ul;

        // TODO: don't ignore annotations
        codes_mapping_get_lp_info2(lp->gid, &group, NULL, NULL, &rep,
                &offset);
        servers_per_group = codes_mapping_get_lp_count(group, 1,
                CODES_STORE_LP_NAME, NULL, 1);
        codes_mapping_get_lp_id(group, CODES_STORE_LP_NAME, NULL, 1, rep,
                offset % servers_per_group, &cs_id);
        ns->server_idx_local = codes_mapping_get_lp_relative_id(cs_id, 0, 1);
        assert(ns->server_idx_local < num_servers);
    }
    else
        ns->server_idx_local = -1;

    ns->error_ct = 0;
    ns->op_status_ct = 0;
    ns->op_index_current = 0;
    INIT_QLIST_HEAD(&ns->ops);
    INIT_QLIST_HEAD(&ns->complete_ops);
    INIT_QLIST_HEAD(&ns->file_id_mappings);

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
    cs_client_msg *m = tw_event_data(e);
    msg_set_header(cs_client_magic, CS_CLI_KICKOFF, lp->gid, &m->header);
    tw_event_send(e);

#if CLIENT_DEBUG
    ns->event_num = 0;
    char val[64];
    sprintf(val, "client.log.%d", ns->client_idx);
    ns->fdbg = fopen(val, "w");
    setvbuf(ns->fdbg, NULL, _IONBF, 0);
    assert(ns->fdbg);
#endif
}

void cs_client_event_handler(
        cs_client_state * ns,
        tw_bf * b,
        cs_client_msg * m,
        tw_lp * lp){
    assert(m->header.magic == cs_client_magic);
    
#if CLIENT_DEBUG
    fprintf(ns->fdbg, "event num %d\n", ns->event_num);
#endif
    if (ns->error_ct > 0){
        ns->error_ct++;
        return;
    }

    switch (m->header.event_type){
        case CS_CLI_KICKOFF:
            handle_cs_client_kickoff(ns, b, m, lp);
            break;
        case CS_CLI_RECV_ACK:
            handle_cs_client_recv_ack(ns, b, m, lp);
            break;
        case CS_CLI_WKLD_CONTINUE:
            /* proceed immediately to workload processing */
#if CLIENT_DEBUG
            fprintf(ns->fdbg, "lp %lu got continue\n", lp->gid);
#endif
            ns->op_status_ct--;
            handle_client_wkld_next(ns, b, m, lp);
            break;
        default:
            assert(!"cs_client event type not known");
            break;
    }
#if CLIENT_DEBUG
    ns->event_num++;
#endif
}

void cs_client_rev_handler(
        cs_client_state * ns,
        tw_bf * b,
        cs_client_msg * m,
        tw_lp * lp){
    assert(m->header.magic == cs_client_magic);

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
        case CS_CLI_KICKOFF:
            handle_cs_client_kickoff_rev(ns, b, m, lp);
            break;
        case CS_CLI_RECV_ACK:
            handle_cs_client_recv_ack_rev(ns, b, m, lp);
            break;
        case CS_CLI_WKLD_CONTINUE:
            /* proceed immediately to workload processing */
            handle_client_wkld_next_rev(ns, b, m, lp);
            break;
        default:
            assert(!"cs_client event type not known");
            break;
    }
}

void cs_client_finalize(
        cs_client_state * ns,
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
void handle_cs_client_kickoff(
        cs_client_state * ns,
        tw_bf *b,
        cs_client_msg * m,
        tw_lp * lp){
    handle_client_wkld_next(ns,b,m,lp);
}

void handle_client_wkld_next(
        cs_client_state * ns,
        tw_bf *b,
        cs_client_msg * m,
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
        /* operations that create a codes-store request across the network */

        msg_header h;
        struct codes_store_request r;
        msg_set_header(cs_client_magic, CS_CLI_RECV_ACK, lp->gid, &h);
        m->op_index_prev = ns->op_index_current;

        uint64_t file_id, off, len;
        if (is_open){
            r.type = op.u.open.create_flag ? CSREQ_CREATE : CSREQ_OPEN;
            file_id = op.u.open.file_id;
            len = 1;
            /* NON-ROOT (rank 0) processes - if in shared file mode go
             * immediately into the barrier */
            if (cli_config.open_mode == OPEN_MODE_SHARED && ns->client_idx != 0) {
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
        client_req *req = client_req_init(cli_config.dist_cfg.stripe_factor);
        req->op_index = ns->op_index_current;
        req->issue_time = tw_now(lp);
        req->type = r.type;

        /* from here, either error or success, so go ahead and add to queue */
        qlist_add_tail(&req->ql, &ns->ops); 

        /* generate the oids. Via the stripe_factor=1 trick, this logic is the
         * same regardless of striping, but it will likely change later 
         * NOTE: for the persistent random case, we have a dedicated function */
        if (cli_config.oid_gen_mode == GEN_MODE_PLACEMENT_RANDOM ||
                cli_config.oid_gen_mode == GEN_MODE_PLACEMENT) {
            req->map = get_file_id_mapping(ns, b, file_id, tw_rand_u64, lp->rng,
                    &m->num_rng_calls);
        }
        else {
            req->map->file_id = file_id; 
            req->map->oids =
                malloc(cli_config.dist_cfg.stripe_factor*sizeof(req->map->oids));
            for (int i = 0; i < cli_config.dist_cfg.stripe_factor; i++){
                switch(cli_config.oid_gen_mode){
                    case GEN_MODE_ZERO:
                        assert(i==0); /* can't be called with striping */
                        req->map->oids[i] = 0; 
                        break;
                    case GEN_MODE_WKLD_FILE_ID:
                        req->map->oids[i] = file_id + i;
                        break;
                    case GEN_MODE_WKLD_HASH: ;
                        uint64_t file_id_strip = file_id + i;
                        uint32_t h1 = 0, h2 = 0;
                        bj_hashlittle2(&file_id_strip, sizeof(uint64_t), &h1, &h2);
                        req->map->oids[i] = (((uint64_t)h1)<<32ull) | ((uint64_t)h2); 
                        break;
                    default:
                        tw_error(TW_LOC,
                                "bad map mode for striped request workload");
                }
            }
        }

        if (cli_config.dist_mode == DIST_MODE_SINGLE) {
            req->oid_offs[0] = off;
            req->oid_lens[0] = len;
        }
        else
            map_logical_to_physical_objs(cli_config.dist_cfg.stripe_factor,
                    cli_config.dist_cfg.strip_size, off, len,
                    req->oid_offs, req->oid_lens);

        MN_START_SEQ();
        r.xfer_offset = 0;
        r.xfer_size = 0;
        for (int i = 0; i < cli_config.dist_cfg.stripe_factor; i++) {
            r.oid = req->map->oids[i];
            if (req->oid_lens[i] != 0) {
                r.xfer_offset = req->oid_offs[i];
                r.xfer_size   = req->oid_lens[i];
            }
            else if (!is_open)
                continue;

            codes_store_send_req(&r, oid_map_lookup(r.oid, oid_map_method),
                    lp, cli_mn_id, CODES_MCTX_DEFAULT,
                    striped_req_to_tag(ns->op_index_current, 0), &h, &cli_cb);
            req->status_ct++;
            req->status[i]++;
#if CLIENT_DEBUG
            fprintf(ns->fdbg,
                    "LP %lu sending req for oid %lu to srv %d\n",
                    lp->gid, req->map->oids[i],
                    oid_map_lookup(r.oid, oid_map_method));
#endif
        }
        MN_END_SEQ();

        // in either case, update the opid
        ns->op_index_current++;
    }
    else if (op.op_type == CODES_WK_DELAY || op.op_type == CODES_WK_CLOSE){
        /* operations that simply invoke another workload op */
        double delay = (op.op_type==CODES_WK_DELAY) ? 
            sec_to_nsec(op.u.delay.seconds) : 0.0;
        tw_event *e = codes_event_new(lp->gid, 
                codes_local_latency(lp)+delay, lp);
        cs_client_msg *m_next = tw_event_data(e);
        *m_next = *m;
        m_next->header.event_type = CS_CLI_WKLD_CONTINUE;
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
        cs_client_msg *m_next = tw_event_data(e);
        *m_next = *m;
        m_next->header.event_type = CS_CLI_WKLD_CONTINUE;
        tw_event_send(e);
    }
}

void handle_cs_client_recv_ack_wkld(
        cs_client_state * ns,
        tw_bf * b,
        cs_client_msg * m,
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
                ns->file_ids[f] != req->map->file_id; f++);
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
        if (cli_config.open_mode == OPEN_MODE_SHARED &&
                req->type == CSREQ_OPEN && ns->client_idx == 0) {
            // current implementation - contact the sentinel barrier LP
            // directly, without going through the network
            enter_barrier_event(0, num_clients, ns->client_idx, lp);
            return;
        }
        else
            handle_client_wkld_next(ns, b, m, lp);
        /* move the entry to the completed q */
        qlist_del(ent);
        qlist_add_tail(ent, &ns->complete_ops);
    }
}

void handle_cs_client_recv_ack(
        cs_client_state * ns,
        tw_bf * b,
        cs_client_msg * m,
        tw_lp * lp){

    assert(m->ret == CODES_STORE_OK);

    handle_cs_client_recv_ack_wkld(ns,b,m,lp);
}

void handle_cs_client_kickoff_rev(
        cs_client_state * ns,
        tw_bf * b,
        cs_client_msg * m,
        tw_lp * lp)
{
    handle_client_wkld_next_rev(ns,b,m,lp);
}

void handle_client_wkld_next_rev(
        cs_client_state * ns,
        tw_bf * b,
        cs_client_msg * m,
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
            if (cli_config.open_mode == OPEN_MODE_SHARED &&
                    ns->client_idx != 0) {
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

        if (cli_config.oid_gen_mode == GEN_MODE_PLACEMENT_RANDOM ||
                cli_config.oid_gen_mode == GEN_MODE_PLACEMENT)
            get_file_id_mapping_rc(ns, b, file_id, tw_rand_u64_rc, lp->rng,
                    m->num_rng_calls);
        else
            free(req->map->oids);

        if (cli_config.dist_mode == DIST_MODE_SINGLE){
            codes_store_send_req_rc(cli_mn_id, lp);
        }
        else {
            for (int i = 0; i < cli_config.dist_cfg.stripe_factor; i++) {
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
static void handle_cs_client_recv_ack_rev_wkld(
        cs_client_state * ns,
        tw_bf * b,
        cs_client_msg * m,
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

    if (rid.strip == cli_config.dist_cfg.stripe_factor)
        tw_error(TW_LOC, "client %d: ack in rc not matched\n", ns->client_idx);
    assert(req->status[rid.strip] == 0);

    if (req->status_ct == 0){
        int f;
        for (f = 0; f < ns->num_files && 
                ns->file_ids[f] != req->map->file_id; f++);
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
        if (cli_config.open_mode == OPEN_MODE_SHARED &&
                req->type == CSREQ_OPEN && ns->client_idx == 0)
            enter_barrier_event_rc(lp);
        else
            handle_client_wkld_next_rev(ns,b,m,lp);
    }
    req->status_ct++;
    req->status[rid.strip]++;
}

void handle_cs_client_recv_ack_rev(
        cs_client_state * ns,
        tw_bf * b,
        cs_client_msg * m,
        tw_lp * lp)
{
    handle_cs_client_recv_ack_rev_wkld(ns,b,m,lp);
}

/**** END IMPLEMENTATIONS ****/

static uint64_t tw_rand_u64(void *s /* <-- tw_rng_stream* */)
{
    return tw_rand_ulong(s, 0, ULONG_MAX);
}
static void tw_rand_u64_rc(void *s /* <-- tw_rng_stream* */)
{
    tw_rand_reverse_unif(s);
}

static int striped_req_to_tag(int op_index, int strip)
{
    return op_index * cli_config.dist_cfg.stripe_factor + strip;
}
static struct striped_req_id tag_to_striped_req(int tag)
{
    struct striped_req_id rtn;
    rtn.op_index = tag / cli_config.dist_cfg.stripe_factor;
    rtn.strip    = tag % cli_config.dist_cfg.stripe_factor;
    return rtn;
}

static file_map* get_file_id_mapping(
        cs_client_state *ns,
        tw_bf *b,
        uint64_t file_id,
        uint64_t (*rng_fn)(void *),
        void * rng_arg,
        int *num_rng_calls)
{
    /* attempt to find an already-generated file id */
    struct qlist_head *ent;
    qlist_for_each(ent, &ns->file_id_mappings){
        file_map *tmp = qlist_entry(ent,file_map,ql);
        if (tmp->file_id == file_id){
            *num_rng_calls = 0;
            return tmp;
        }
        b->c0 = 1;
    }

    file_map *fm = malloc(sizeof(file_map));
    fm->oids = malloc(cli_config.dist_cfg.stripe_factor*sizeof(*fm->oids));
    assert(fm->oids);
    fm->file_id = file_id;

    int start_svr, end_svr;
    uint64_t (*rfn)(void *);
    void * rarg;
    if (cli_config.placement_mode == PLC_MODE_LOCAL) {
        start_svr = ns->server_idx_local;
        end_svr   = ns->server_idx_local + 1;
        rfn = NULL;
        rarg = NULL;
    }
    else {
        start_svr = 0;
        end_svr = num_servers;
        if (cli_config.oid_gen_mode == GEN_MODE_PLACEMENT) {
            rfn = NULL;
            rarg = NULL;
        }
        else {
            rfn = rng_fn;
            rarg = rng_arg;
        }
    }
    *num_rng_calls =
        oid_map_generate_striped_random(cli_config.dist_cfg.stripe_factor,
                start_svr, end_svr, fm->oids, rfn, rarg,
                oid_map_method);
    if (*num_rng_calls == -1)
        tw_error(TW_LOC,
                "client id %d: unable to create oid mapping\n",
                ns->client_idx);
    return fm;
}

static void get_file_id_mapping_rc(
        cs_client_state *ns,
        tw_bf *b,
        uint64_t file_id,
        void (*rng_fn_rc) (void *),
        void * rng_arg,
        int num_rng_calls)
{
    if (!b->c0) {
        struct qlist_head *ent;
        qlist_for_each(ent, &ns->file_id_mappings) {
            file_map *tmp = qlist_entry(ent, file_map, ql);
            if (tmp->file_id == file_id) {
                qlist_del(ent);
                free(tmp->oids);
                free(tmp);
                break;
            }
        }
    }

    oid_map_generate_striped_random_rc(num_rng_calls, rng_fn_rc, rng_arg);
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
