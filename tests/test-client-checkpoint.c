/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <stdio.h>
#include <stddef.h>
#include <assert.h>

#include <ross.h>
#include <codes/codes_mapping.h>
#include <codes/jenkins-hash.h>
#include <codes/codes-workload.h>
#include <codes/model-net.h>

#include "../codes/codes-store-lp.h"
#include "test-checkpoint.h"

#define CHK_LP_NM "test-checkpoint-client"
#define MEAN_INTERVAL 1000000.0
#define CLIENT_DBG 1
#define MAX_PAYLOAD_SZ 2048
#define TRACK 0
#define dprintf(_fmt, ...) \
    do {if (CLIENT_DBG) printf(_fmt, __VA_ARGS__);} while (0)

/* checkpoint restart parameters */
static double checkpoint_sz;
static double checkpoint_wr_bw;
static double app_run_time;
static double mtti;

static int test_checkpoint_magic;
static int cli_dfly_id;
// following is for mapping clients to servers
static int num_servers;
static int num_clients;
static int clients_per_server;

static checkpoint_wrkld_params c_params = {0, 0, 0, 0, 0};

static tw_stime ns_to_s(tw_stime ns);
static tw_stime s_to_ns(tw_stime ns);

enum test_checkpoint_event
{
    CLI_NEXT_OP=12,
    CLI_ACK,
    CLI_BCKGND_GEN
};

struct test_checkpoint_state
{
    int cli_rel_id;
    int num_sent_wr;
    int num_sent_rd;
    struct codes_cb_info cb;
    int wkld_id;
    tw_stime start_time;
    tw_stime completion_time;
    int op_status_ct;
    int error_ct;
    tw_stime delayed_time;
    int num_completed_ops;
    
    tw_stime write_start_time;
    tw_stime total_write_time;

    uint64_t read_size;
    uint64_t write_size;
    
    int num_reads;
    int num_writes;

    char output_buf[512];
};

struct test_checkpoint_msg
{
    msg_header h;
    int payload_sz;
    int tag;
    codes_store_ret_t ret;
    struct codes_workload_op op_rc;
    tw_stime saved_delay_time;
    tw_stime saved_write_time;
};

/* convert ns to seconds */
static tw_stime ns_to_s(tw_stime ns)
{
    return(ns / (1000.0 * 1000.0 * 1000.0));
}

/* convert seconds to ns */
static tw_stime s_to_ns(tw_stime ns)
{
    return(ns * (1000.0 * 1000.0 * 1000.0));
}

static void send_req_to_store_rc(
	struct test_checkpoint_state * ns,
        tw_lp * lp)
{
	codes_store_send_req_rc(cli_dfly_id, lp);	
}

static void send_req_to_store(
	struct test_checkpoint_state * ns,
        tw_lp * lp,
        struct test_checkpoint_msg * m,
        int is_write)
{
    struct codes_store_request r;
    msg_header h;

    codes_store_init_req(
            is_write? CSREQ_WRITE:CSREQ_READ, 0, 0, 
	    is_write? m->op_rc.u.write.offset:m->op_rc.u.read.offset, 
            is_write? m->op_rc.u.write.size:m->op_rc.u.read.size, 
            &r);

    msg_set_header(test_checkpoint_magic, CLI_ACK, lp->gid, &h);

    int dest_server_id = ns->cli_rel_id / clients_per_server;
   
    codes_store_send_req(&r, dest_server_id, lp, cli_dfly_id, CODES_MCTX_DEFAULT,
            0, &h, &ns->cb);

    if(lp->gid == TRACK)
        dprintf("%lu: sent %s request\n", lp->gid, is_write ? "write" : "read");

    ns->write_start_time = tw_now(lp);
    ns->write_size += m->op_rc.u.write.size;
}

void handle_next_operation_rc(
	struct test_checkpoint_state * ns, 
	tw_lp * lp)
{
	ns->op_status_ct++;
}

/* Add a certain delay event */
void handle_next_operation(
	struct test_checkpoint_state * ns,
	tw_lp * lp,
	tw_stime time)
{
    ns->op_status_ct--;
   /* Issue another next event after a certain time */
    tw_event * e;

    struct test_checkpoint_msg * m_new;
    e = codes_event_new(lp->gid, time, lp);
    m_new = tw_event_data(e);
    msg_set_header(test_checkpoint_magic, CLI_NEXT_OP, lp->gid, &m_new->h);    
    tw_event_send(e);

    return;
}

void generate_random_traffic_rc(
      struct test_checkpoint_state * ns,
      tw_bf * b,
      struct test_checkpoint_msg * msg,
      tw_lp * lp)
{
    tw_rand_reverse_unif(lp->rng);
    tw_rand_reverse_unif(lp->rng);
    
    model_net_event_rc(cli_dfly_id, lp, msg->payload_sz);

    if(b->c1)
        codes_local_latency_reverse(lp);

    if(b->c0)
	    handle_next_operation_rc(ns, lp);
}

void generate_random_traffic(
    struct test_checkpoint_state * ns,
    tw_bf * b,
    struct test_checkpoint_msg * msg,
    tw_lp * lp)
{
   b->c0 = 0;
   b->c1 = 0;

   char anno[MAX_NAME_LENGTH];
   struct test_checkpoint_msg * m_remote = malloc(sizeof(struct test_checkpoint_msg));
   msg_set_header(test_checkpoint_magic, CLI_BCKGND_GEN, lp->gid, &(m_remote->h));

   char lp_grp_name[MAX_NAME_LENGTH];
   char lp_name[MAX_NAME_LENGTH];

   int mapping_gid, mapping_tid, mapping_rid, mapping_offset;
   codes_mapping_get_lp_info(lp->gid, lp_grp_name, &mapping_gid, lp_name, &mapping_tid, NULL, &mapping_rid, &mapping_offset);

   int num_clients = codes_mapping_get_lp_count(NULL, 0,
           lp_name, NULL, 1);

   tw_lpid dest_gid;
   tw_lpid dest_svr = tw_rand_integer(lp->rng, 0, num_clients - 1);
   codes_mapping_get_lp_id(lp_grp_name, lp_name, anno, 1, dest_svr, 0, &dest_gid );

   int payload_sz = tw_rand_integer(lp->rng, 0, MAX_PAYLOAD_SZ);
   model_net_event(cli_dfly_id, "test", dest_gid, payload_sz, 0.0, 
           sizeof(struct test_checkpoint_msg), (const void*)m_remote, 
           0, NULL, lp);
   msg->payload_sz = payload_sz;

   if(tw_now(lp) < ns->delayed_time)
   {
        b->c1 = 1;
        tw_stime ts = MEAN_INTERVAL + tw_rand_exponential(lp->rng, MEAN_INTERVAL); 
        //if(lp->gid == TRACK)
        //    printf("\n Received background traffic scheduling another after %lf ", tw_now(lp) + ts);
        tw_event * e;
        struct test_checkpoint_msg * m_new;
        e = codes_event_new(lp->gid, ts, lp);
        m_new = tw_event_data(e);
        msg_set_header(test_checkpoint_magic, CLI_BCKGND_GEN, lp->gid, &(m_new->h));    
        tw_event_send(e);
   }
   else
   {
      /* delay has ended, call next event */
      b->c0 = 1;
      handle_next_operation(ns, lp, codes_local_latency(lp));
   }
}

static void next(
        struct test_checkpoint_state * ns,
        tw_bf * bf,
        struct test_checkpoint_msg * msg,
        tw_lp * lp)
{
    if(ns->op_status_ct > 0)
    {
        char buf[64];
        int written = sprintf(buf, "I/O workload operator error: %d  \n",lp->gid);

        lp_io_write(lp->gid, "errors", written, buf);
        return;
    }

    ns->op_status_ct++;

    struct codes_workload_op op_rc;
    codes_workload_get_next(ns->wkld_id, 0, ns->cli_rel_id, &op_rc);

    /* save the op in the message */
    msg->op_rc = op_rc;
   
    if(op_rc.op_type == CODES_WK_END)
    {
		ns->completion_time = tw_now(lp);
        
        if(lp->gid == TRACK)
            dprintf("Client rank %d completed workload.\n", ns->cli_rel_id);
		
        return;
    
    }
    switch(op_rc.op_type)
   {
	case CODES_WK_BARRIER:
	{
        if(lp->gid == TRACK)
		    dprintf("Client rank %d hit barrier.\n", ns->cli_rel_id);
		
        handle_next_operation(ns, lp, codes_local_latency(lp));
	}
    break;

	case CODES_WK_DELAY:
	{
        if(lp->gid == TRACK)
		    dprintf("Client rank %d will delay for %lf seconds.\n", ns->cli_rel_id,
                msg->op_rc.u.delay.seconds);
                tw_stime nano_secs = s_to_ns(msg->op_rc.u.delay.seconds);
#if GENERATE_TRAFFIC       
        msg->saved_delay_time = ns->delayed_time;
        ns->delayed_time = tw_now(lp) + nano_secs;
   
        /* Generate random traffic during the delay */
        tw_event * e;
        struct test_checkpoint_msg * m_new;
        tw_stime ts = MEAN_INTERVAL + tw_rand_exponential(lp->rng, MEAN_INTERVAL);
        e = codes_event_new(lp->gid, ts, lp);
        m_new = tw_event_data(e);
        msg_set_header(test_checkpoint_magic, CLI_BCKGND_GEN, lp->gid, &m_new->h);    
        tw_event_send(e);
#else
        handle_next_operation(ns, lp, nano_secs);
#endif
	}
	break;

    case CODES_WK_OPEN:
	{
        if(lp->gid == TRACK)
		    dprintf("Client rank %d will open file id %ld \n ", ns->cli_rel_id,
		
        msg->op_rc.u.open.file_id);
		handle_next_operation(ns, lp, codes_local_latency(lp));
	}
	break;
	
	case CODES_WK_CLOSE:
	{	
        if(lp->gid == TRACK)
		    dprintf("Client rank %d will close file id %ld \n ", ns->cli_rel_id,
		
        msg->op_rc.u.close.file_id);
		handle_next_operation(ns, lp, codes_local_latency(lp));
	}
	break;
	case CODES_WK_WRITE:
	{
        if(lp->gid == TRACK)
		    dprintf("Client rank %d initiate write operation size %ld offset %ld .\n", ns->cli_rel_id, 
		
        msg->op_rc.u.write.size, msg->op_rc.u.write.offset);
		send_req_to_store(ns, lp, msg, 1);
    }	
	break;

	case CODES_WK_READ:
	{
        if(lp->gid == TRACK)
		    dprintf("Client rank %d initiate write operation size %ld offset %ld .\n", ns->cli_rel_id, msg->op_rc.u.read.size, msg->op_rc.u.read.offset);
                ns->num_sent_rd++;
		send_req_to_store(ns, lp, msg, 0);
    }
	break;

	default:
	      dprintf("\n Unknown client operation %d ", msg->op_rc.op_type);
      }
	
}
static void next_rc(
        struct test_checkpoint_state *ns,
        tw_bf * bf,
        struct test_checkpoint_msg *m,
        tw_lp *lp)
{
    ns->op_status_ct--;
    codes_workload_get_next_rc(ns->wkld_id, 0, ns->cli_rel_id, &m->op_rc);

    if(m->op_rc.op_type == CODES_WK_END)
        return;

    switch(m->op_rc.op_type) 
    {
      case CODES_WK_READ:
      {
	    ns->num_sent_rd--;
        send_req_to_store_rc(ns, lp);
      }
      break;

      case CODES_WK_WRITE:
      {
         ns->num_sent_wr--;
         send_req_to_store_rc(ns, lp);
      }
      break;

      case CODES_WK_DELAY:
      {
 #ifdef GENERATE_TRAFFIC 
         ns->delayed_time = m->saved_delay_time;
         tw_rand_reverse_unif(lp->rng);
#else
         handle_next_operation_rc(ns, lp); 
#endif
      }
      break;
      case CODES_WK_BARRIER:
      case CODES_WK_OPEN:
      case CODES_WK_CLOSE:
      {
    	codes_local_latency_reverse(lp);
	    handle_next_operation_rc(ns, lp);
      }
      break;


     default:
	printf("\n Unknown client operation reverse %d", m->op_rc.op_type);
    }
    
}


static void test_checkpoint_event(
        struct test_checkpoint_state * ns,
        tw_bf * b,
        struct test_checkpoint_msg * m,
        tw_lp * lp)
{
#if CLIENT_DEBUG
    fprintf(ns->fdbg, "event num %d\n", ns->event_num);
#endif
    if (ns->error_ct > 0){
        ns->error_ct++;
        return;
    }
    assert(m->h.magic == test_checkpoint_magic);

    switch(m->h.event_type) {
	case CLI_NEXT_OP:
		next(ns, b, m, lp);
	break;

	case CLI_ACK:
        ns->num_completed_ops++;
        if(lp->gid == TRACK)
		    dprintf("\n !!!! Ack %d received from store %lf client %ld ", ns->num_completed_ops, tw_now(lp), lp->gid);
		handle_next_operation(ns, lp, codes_local_latency(lp));
	    m->saved_write_time = ns->total_write_time;
        ns->total_write_time += (tw_now(lp) - ns->write_start_time);
    break;

    case CLI_BCKGND_GEN:
        generate_random_traffic(ns, b, m, lp);
        break;

        default:
            assert(0);
    }
}
static void test_checkpoint_event_rc(
        struct test_checkpoint_state * ns,
        tw_bf * b,
        struct test_checkpoint_msg * m,
        tw_lp * lp)
{
    assert(m->h.magic == test_checkpoint_magic);

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
    switch(m->h.event_type) {
	case CLI_NEXT_OP:	
		next_rc(ns, b, m, lp);
	break;
	
	case CLI_ACK:
		ns->num_completed_ops--;
        ns->total_write_time = m->saved_write_time;
        handle_next_operation_rc(ns, lp);
	break;

    case CLI_BCKGND_GEN:
        generate_random_traffic_rc(ns, b, m, lp);
        break;

        default:
            assert(0);
    }
}

static void test_checkpoint_init(
        struct test_checkpoint_state * ns,
        tw_lp * lp)
{ 
    ns->op_status_ct = 0;
    ns->error_ct = 0;
    ns->num_sent_wr = 0;
    ns->num_sent_rd = 0;
    ns->delayed_time = 0.0;
    ns->total_write_time = 0.0;

    ns->num_reads = 0;
    ns->num_writes = 0;
    ns->read_size = 0;
    ns->write_size = 0;

    INIT_CODES_CB_INFO(&ns->cb, struct test_checkpoint_msg, h, tag, ret);

    ns->cli_rel_id = codes_mapping_get_lp_relative_id(lp->gid, 0, 0);
    ns->start_time = tw_now(lp);

    printf("\n Client ID LP ID %d ", lp->gid);
    handle_next_operation(ns, lp, codes_local_latency(lp));
}

static void test_checkpoint_pre_run(
        struct test_checkpoint_state *ns,
        tw_lp *lp)
{
      char* w_params = (char*)&c_params;
      ns->wkld_id = codes_workload_load("checkpoint_io_workload", w_params, 0, ns->cli_rel_id);
}

static void test_checkpoint_finalize(
        struct test_checkpoint_state *ns,
        tw_lp *lp)
{
    /*if (ns->num_complete_wr != num_reqs)
        tw_error(TW_LOC, "num_complete_wr:%d does not match num_reqs:%d\n",
                ns->num_complete_wr, num_reqs);
    if (ns->num_complete_rd != num_reqs)
        tw_error(TW_LOC, "num_complete_rd:%d does not match num_reqs:%d\n",
                ns->num_complete_rd, num_reqs);
    */
   int written = 0;
   if(!ns->cli_rel_id)
      written = sprintf(ns->output_buf, "# Format <LP id> <Bytes written> <Time to write bytes >");
   
   written += sprintf(ns->output_buf + written, "%lu %lu %ld %lf \n", lp->gid, ns->cli_rel_id, ns->write_size, ns->total_write_time);
   lp_io_write(lp->gid, "checkpoint-client-stats", written, ns->output_buf);   
}

tw_lptype test_checkpoint_lp = {
    (init_f) test_checkpoint_init,
    (pre_run_f) test_checkpoint_pre_run,
    (event_f) test_checkpoint_event,
    (revent_f) test_checkpoint_event_rc,
    (final_f) test_checkpoint_finalize,
    (map_f) codes_mapping,
    sizeof(struct test_checkpoint_state),
};

void test_checkpoint_register(){
    lp_type_register(CHK_LP_NM, &test_checkpoint_lp);
}

void test_checkpoint_configure(int model_net_id){
    uint32_t h1=0, h2=0;

    bj_hashlittle2(CHK_LP_NM, strlen(CHK_LP_NM), &h1, &h2);
    test_checkpoint_magic = h1+h2;
    cli_dfly_id = model_net_id;

    int rc;
    rc = configuration_get_value_double(&config, "test-checkpoint-client", "checkpoint_sz", NULL,
	   &c_params.checkpoint_sz);
    assert(!rc);

    rc = configuration_get_value_double(&config, "test-checkpoint-client", "checkpoint_wr_bw", NULL,
	   &c_params.checkpoint_wr_bw);
    assert(!rc);

    rc = configuration_get_value_double(&config, "test-checkpoint-client", "app_run_time", NULL,
	   &c_params.app_runtime);
    assert(!rc);

    rc = configuration_get_value_double(&config, "test-checkpoint-client", "mtti", NULL,
	   &c_params.mtti);
    assert(!rc);
   
    num_servers =
        codes_mapping_get_lp_count(NULL, 0, CODES_STORE_LP_NAME, NULL, 1);
    num_clients =
        codes_mapping_get_lp_count(NULL, 0, CHK_LP_NM, NULL, 1);

    dprintf("\n Number of clients %d ", num_clients);
    c_params.nprocs = num_clients;

    clients_per_server = num_clients / num_servers;
    if (clients_per_server == 0)
        clients_per_server = 1;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 *  indent-tabs-mode: nil
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
