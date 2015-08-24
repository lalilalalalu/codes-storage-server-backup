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

#include "../codes/codes-store-lp.h"
#include "test-checkpoint.h"

#define CHK_LP_NM "test-checkpoint-client"

#define CLIENT_DBG 0
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
static int do_server_mapping = 0;
static int num_servers;
static int num_clients;
static int clients_per_server;

static checkpoint_wrkld_params c_params = {0, 0, 0, 0, 0};

static tw_stime ns_to_s(tw_stime ns);
static tw_stime s_to_ns(tw_stime ns);

enum test_checkpoint_event
{
    TEST_CLI_ACK = 12,
    CLI_NEXT_OP
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
};

struct test_checkpoint_msg
{
    msg_header h;
    int tag;
    codes_store_ret_t ret;
    struct codes_workload_op op_rc;
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

    msg_set_header(test_checkpoint_magic, TEST_CLI_ACK, lp->gid, &h);

    int dest_server_id;
    if (do_server_mapping)
        dest_server_id = ns->cli_rel_id / clients_per_server;
    else
        dest_server_id = 0;
   
    codes_store_send_req(&r, dest_server_id, lp, cli_dfly_id, CODES_MCTX_DEFAULT,
            0, &h, &ns->cb);

    dprintf("%lu: sent %s request\n", lp->gid, is_write ? "write" : "read");
   
}

/* Add a certain delay event */
void handle_next_operation(
	struct test_checkpoint_state * ns,
	tw_lp * lp,
	tw_stime time)
{
   /* Issue another next event after a certain time */
    tw_event * e;

    struct test_checkpoint_msg * m_new;
    e = codes_event_new(lp->gid, time, lp);
    m_new = tw_event_data(e);
    msg_set_header(test_checkpoint_magic, CLI_NEXT_OP, lp->gid, &m_new->h);    
    tw_event_send(e);

    return;
}

static void next(
        struct test_checkpoint_state * ns,
        struct test_checkpoint_msg * m,
        tw_lp * lp)
{
    m = malloc(sizeof(struct test_checkpoint_msg));
    codes_workload_get_next(ns->wkld_id, 0, ns->cli_rel_id, &m->op_rc);

      switch(m->op_rc.op_type)
      {
	case CODES_WK_END:
	{
		ns->completion_time = tw_now(lp);
		dprintf("Client rank %d completed workload.\n", ns->cli_rel_id);
		return;
	}
	break;

	case CODES_WK_BARRIER:
	{
		dprintf("Client rank %d hit barrier.\n", ns->cli_rel_id);
		handle_next_operation(ns, lp, codes_local_latency(lp));
	}
        break;

	case CODES_WK_DELAY:
	{
		dprintf("Client rank %d will delay for %lf seconds.\n", ns->cli_rel_id,
                m->op_rc.u.delay.seconds);
                tw_stime nano_secs = s_to_ns(m->op_rc.u.delay.seconds);
		handle_next_operation(ns, lp, nano_secs);
	}
	break;

        case CODES_WK_OPEN:
	{
		dprintf("Client rank %d will open file id %ld \n ", ns->cli_rel_id,
		m->op_rc.u.open.file_id);
		handle_next_operation(ns, lp, codes_local_latency(lp));
	}
	break;
	case CODES_WK_WRITE:
	{
		dprintf("Client rank %d initiate write operation size %ld offset %ld .\n", ns->cli_rel_id, 
		m->op_rc.u.write.size, m->op_rc.u.write.offset);
		send_req_to_store(ns, lp, m, 1);
	}	
	break;

	case CODES_WK_READ:
	{
		dprintf("Client rank %d initiate write operation size %ld offset %ld .\n", ns->cli_rel_id, m->op_rc.u.read.size, m->op_rc.u.read.offset);
                ns->num_sent_rd++;
		send_req_to_store(ns, lp, m, 0);
	}
	break;

	default:
	      dprintf("\n Unknown client operation %d ", m->op_rc.op_type);
      }
	
}
static void next_rc(
        struct test_checkpoint_state *ns,
        struct test_checkpoint_msg *m,
        tw_lp *lp)
{
    codes_workload_get_next_rc(ns->wkld_id, 0, ns->cli_rel_id, &m->op_rc);

    switch(m->op_rc.op_type) 
    {
      case CODES_WK_READ:
      {
	ns->num_sent_rd--;
        codes_store_send_req_rc(cli_dfly_id, lp);
      }
      break;

      case CODES_WK_WRITE:
      {
         ns->num_sent_wr--;
         codes_store_send_req_rc(cli_dfly_id, lp);
      }
      break;

      case CODES_WK_DELAY:
      case CODES_WK_BARRIER:
      case CODES_WK_OPEN:
      {
	codes_local_latency_reverse(lp);
      }
      break;

      case CODES_WK_END:
      {
      /* Do nothing */
      }
      break;

     default:
	printf("\n Unknown client operation reverse ", m->op_rc.op_type);
    }
}


static void test_checkpoint_event(
        struct test_checkpoint_state * ns,
        tw_bf * b,
        struct test_checkpoint_msg * m,
        tw_lp * lp)
{
    assert(m->h.magic == test_checkpoint_magic);

    switch(m->h.event_type) {
        case TEST_CLI_ACK:
              dprintf("%lu: received ack\n", lp->gid);
              next(ns, m, lp);
            break;
	case CLI_NEXT_OP:
//		dprintf(" %lu: next op\n", lp->gid);	
		next(ns, m, lp);
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

    switch(m->h.event_type) {
        case TEST_CLI_ACK:
              dprintf("%lu: received ack (rc)\n", lp->gid);
              assert(m->ret == CODES_STORE_OK);
              next_rc(ns, m, lp);
            break;
	case CLI_NEXT_OP:	
		next_rc(ns, m, lp);
	break;
        default:
            assert(0);
    }
}

static void test_checkpoint_init(
        struct test_checkpoint_state * ns,
        tw_lp * lp)
{
    ns->num_sent_wr = 0;
    ns->num_sent_rd = 0;
    INIT_CODES_CB_INFO(&ns->cb, struct test_checkpoint_msg, h, tag, ret);

    ns->cli_rel_id = codes_mapping_get_lp_relative_id(lp->gid, 0, 0);
    ns->start_time = tw_now(lp);
}

static void test_checkpoint_pre_run(
        struct test_checkpoint_state *ns,
        tw_lp *lp)
{
      char* w_params = (char*)&c_params;
      ns->wkld_id = codes_workload_load("checkpoint_io_workload", w_params, 0, ns->cli_rel_id);
      next(ns, NULL, lp);
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
   
    configuration_get_value_int(&config, "test-checkpoint-client", "do_server_mapping", NULL,
            &do_server_mapping);

    num_servers =
        codes_mapping_get_lp_count(NULL, 0, CODES_STORE_LP_NAME, NULL, 1);
    num_clients =
        codes_mapping_get_lp_count(NULL, 0, CHK_LP_NM, NULL, 1);

    printf("\n Number of clients %d ", num_clients);
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