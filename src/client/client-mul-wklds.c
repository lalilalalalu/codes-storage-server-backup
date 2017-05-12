/* Copyright (C) 2015 University of Chicago.
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
#include <codes/codes-jobmap.h>
#include <codes/resource-lp.h>
#include <codes/local-storage-model.h>
#include "../codes/codes-external-store.h"
#include "../codes/codes-store-lp.h"

#define CHK_LP_NM "test-checkpoint-client"
#define MEAN_INTERVAL 550055
#define CLIENT_DBG 0
#define TRACK 0
#define MAX_JOBS 5
#define dprintf(_fmt, ...) \
    do {if (CLIENT_DBG) printf(_fmt, __VA_ARGS__);} while (0)

void test_checkpoint_register(void);

/* configures the lp given the global config object */
void test_checkpoint_configure(int model_net_id);

static char lp_io_dir[256] = {'\0'};
static unsigned int lp_io_use_suffix = 0;
static int do_lp_io = 0;
static int extrapolate_factor = 10;
static lp_io_handle io_handle;

static char workloads_conf_file[4096] = {'\0'};
static char alloc_file[4096] = {'\0'};
static char conf_file_name[4096] = {'\0'};
static int random_bb_nodes = 0;
static int total_checkpoints = 0;
static int PAYLOAD_SZ = 1024;
static unsigned long max_gen_data = 5000000000;

char anno[MAX_NAME_LENGTH];
char lp_grp_name[MAX_NAME_LENGTH];
char lp_name[MAX_NAME_LENGTH];
static int mapping_gid, mapping_tid, mapping_rid, mapping_offset;

int num_jobs_per_wkld[MAX_JOBS];
char wkld_type_per_job[MAX_JOBS][4096];

struct codes_jobmap_ctx *jobmap_ctx;
struct codes_jobmap_params_list jobmap_p;

/* checkpoint restart parameters */
static double checkpoint_sz;
static double checkpoint_wr_bw;
static double mtti;

static int test_checkpoint_magic;
static int cli_dfly_id;

// following is for mapping clients to servers
static int num_servers;
static int num_clients;
static int clients_per_server;
static int num_syn_clients;
static int num_chk_clients;

static double my_checkpoint_sz;
static double intm_sleep = 0;
static tw_stime max_write_time = 0;
static tw_stime max_total_time = 0;
static tw_stime total_write_time = 0;
static tw_stime total_run_time = 0;
static double total_syn_data = 0;
static int num_completed = 0;

static checkpoint_wrkld_params c_params = {0, 0, 0, 0, 0};

static tw_stime ns_to_s(tw_stime ns);
static tw_stime s_to_ns(tw_stime ns);

enum test_checkpoint_event
{
    CLI_NEXT_OP=12,
    CLI_ACK,
    CLI_WKLD_FINISH,
    CLI_BCKGND_GEN,
    CLI_BCKGND_COMPLETE,
    CLI_BCKGND_FINISH
};

struct test_checkpoint_state
{
    int cli_rel_id;
    int num_sent_wr;
    int num_sent_rd;
    struct codes_cb_info cb;
    int wkld_id;
    int is_finished;

    tw_stime start_time;
    tw_stime completion_time;
    int op_status_ct;
    int error_ct;
    tw_stime delayed_time;
    int num_completed_ops;
    int neighbor_completed;

    /* Number of bursts for the uniform random traffic */
    int num_bursts;

    tw_stime write_start_time;
    tw_stime total_write_time;

    uint64_t read_size;
    uint64_t write_size;


    int num_reads;
    int num_writes;

    char output_buf[512];
    int64_t syn_data_sz;
    int64_t gen_data_sz;

    /* For randomly selected BB nodes */
    int random_bb_node_id;

    /* For running multiple workloads */
    int app_id;
    int local_rank;
};

struct test_checkpoint_msg
{
    model_net_event_return event_rc;
    msg_header h;
    int payload_sz;
    int tag;
    int saved_data_sz;
    uint64_t saved_write_sz;
    int saved_op_type;
    codes_store_ret_t ret;
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

static double terabytes_to_megabytes(double tib)
{
    return (tib * 1000.0 * 1000.0);
}

static void kickoff_synthetic_traffic_rc(
        struct test_checkpoint_state * ns,
        tw_lp * lp)
{
    tw_rand_reverse_unif(lp->rng);
}

static void kickoff_synthetic_traffic(
        struct test_checkpoint_state * ns,
        tw_lp * lp)
{
        ns->is_finished = 0;

        double checkpoint_wr_time = (c_params.checkpoint_sz * 1024)/ c_params.checkpoint_wr_bw;
        tw_stime checkpoint_interval = sqrt(2 * checkpoint_wr_time * (c_params.mtti * 60 * 60)) - checkpoint_wr_time;
   
        if(!ns->local_rank)
            printf("\n Delay for synthetic traffic %lf ", checkpoint_interval - 2.0); 

        assert(checkpoint_interval > 0);
        tw_stime nano_secs = s_to_ns(checkpoint_interval - 2.0);
        ns->start_time = tw_now(lp) + nano_secs;
        /* Generate random traffic during the delay */
        tw_event * e;
        struct test_checkpoint_msg * m_new;
        tw_stime ts = nano_secs + tw_rand_unif(lp->rng);
        e = tw_event_new(lp->gid, ts, lp);
        m_new = tw_event_data(e);
        msg_set_header(test_checkpoint_magic, CLI_BCKGND_GEN, lp->gid, &m_new->h);    
        tw_event_send(e);
}

static void notify_background_traffic_rc(
	    struct test_checkpoint_state * ns,
        tw_lp * lp,
        tw_bf * bf)
{
    tw_rand_reverse_unif(lp->rng); 
}
static void notify_background_traffic(
	    struct test_checkpoint_state * ns,
        tw_lp * lp,
        tw_bf * bf,
        struct test_checkpoint_msg * m)
{
        bf->c0 = 1;
        
        struct codes_jobmap_id jid; 
        jid = codes_jobmap_to_local_id(ns->cli_rel_id, jobmap_ctx);
        
        /* TODO: Assuming there are two jobs */    
        int other_id = 0;
        if(jid.job == 0)
            other_id = 1;
        
        struct codes_jobmap_id other_jid;
        other_jid.job = other_id;

        int num_other_ranks = codes_jobmap_get_num_ranks(other_id, jobmap_ctx);

        tw_stime ts = (1.1 * g_tw_lookahead) + tw_rand_unif(lp->rng);
        tw_lpid global_dest_id;
 
        dprintf("\n Checkpoint LP %llu lpid %d notifying background traffic!!! ", lp->gid, ns->local_rank);
        for(int k = 0; k < num_other_ranks; k++)    
        {
            other_jid.rank = k;
            int intm_dest_id = codes_jobmap_to_global_id(other_jid, jobmap_ctx); 
            global_dest_id = codes_mapping_get_lpid_from_relative(intm_dest_id, NULL, CHK_LP_NM, NULL, 0);

            tw_event * e;
            struct test_checkpoint_msg * m_new;  
            e = tw_event_new(global_dest_id, ts, lp);
            m_new = tw_event_data(e); 
            msg_set_header(test_checkpoint_magic, CLI_BCKGND_FINISH, lp->gid, &(m_new->h));
            tw_event_send(e);   
        }
        return;
}
static void notify_neighbor_rc(
	    struct test_checkpoint_state * ns,
        tw_lp * lp,
        tw_bf * bf)
{
       if(bf->c0)
       {
            notify_background_traffic_rc(ns, lp, bf);
       }
   
       if(bf->c1)
       {
          tw_rand_reverse_unif(lp->rng); 
       }
} 
static void notify_neighbor(
	    struct test_checkpoint_state * ns,
        tw_lp * lp,
        tw_bf * bf,
        struct test_checkpoint_msg * m)
{
    bf->c0 = 0;
    bf->c1 = 0;

    if(ns->local_rank == num_chk_clients - 1 
            && ns->is_finished == 1)
    {
        bf->c0 = 1;
        notify_background_traffic(ns, lp, bf, m);
        return;
    }
    
    struct codes_jobmap_id nbr_jid;
    nbr_jid.job = ns->app_id;
    tw_lpid global_dest_id;

    if(ns->is_finished == 1 && (ns->neighbor_completed == 1 || ns->local_rank == 0))
    {
        bf->c1 = 1;

        dprintf("\n Local rank %d notifying neighbor %d ", ns->local_rank, ns->local_rank+1);
        tw_stime ts = (1.1 * g_tw_lookahead) + tw_rand_unif(lp->rng);
        nbr_jid.rank = ns->local_rank + 1;
        
        /* Send a notification to the neighbor about completion */
        int intm_dest_id = codes_jobmap_to_global_id(nbr_jid, jobmap_ctx); 
        global_dest_id = codes_mapping_get_lpid_from_relative(intm_dest_id, NULL, CHK_LP_NM, NULL, 0);
       
        tw_event * e;
        struct test_checkpoint_msg * m_new;  
        e = tw_event_new(global_dest_id, ts, lp);
        m_new = tw_event_data(e); 
        msg_set_header(test_checkpoint_magic, CLI_WKLD_FINISH, lp->gid, &(m_new->h));
        tw_event_send(e);   
    }
}

static void send_req_to_store_rc(
	struct test_checkpoint_state * ns,
        tw_lp * lp,
        tw_bf * bf,
        struct test_checkpoint_msg * m)
{
        if(bf->c0)
        {
           tw_rand_reverse_unif(lp->rng);
        }
	    codes_store_send_req_rc(cli_dfly_id, lp);
        
        if(bf->c10)
        {
            ns->write_size = m->saved_write_sz;
        }
}

static void send_req_to_store(
	    struct test_checkpoint_state * ns,
        tw_lp * lp,
        tw_bf * bf,
        struct codes_workload_op * op_rc,
        struct test_checkpoint_msg * m,
        int is_write)
{
    struct codes_store_request r;
    msg_header h;

    codes_store_init_req(
            is_write? CSREQ_WRITE:CSREQ_READ, 0, 0, 
	        is_write? op_rc->u.write.offset:op_rc->u.read.offset, 
            is_write? op_rc->u.write.size:op_rc->u.read.size, 
            &r);

    msg_set_header(test_checkpoint_magic, CLI_ACK, lp->gid, &h);
    int dest_server_id = ns->cli_rel_id / clients_per_server;

    if(random_bb_nodes == 1)
    {
            if(ns->random_bb_node_id == -1)
            {
                bf->c0 = 1;
                dest_server_id = tw_rand_integer(lp->rng, 0, num_servers - 1);
                ns->random_bb_node_id = dest_server_id;
            }
            else
                dest_server_id = ns->random_bb_node_id;
    }

    //printf("\n cli %d dest server id %d ", ns->cli_rel_id, dest_server_id);

    codes_store_send_req(&r, dest_server_id, lp, cli_dfly_id, CODES_MCTX_DEFAULT,
            0, &h, &ns->cb);

    //if(ns->cli_rel_id == TRACK)
    //    tw_output(lp, "%llu: sent %s request\n", lp->gid, is_write ? "write" : "read");

    if(is_write)
    {
        bf->c10 = 1;
        m->saved_write_sz = ns->write_size;
        ns->write_start_time = tw_now(lp);
        ns->write_size += op_rc->u.write.size;
    }
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
    e = tw_event_new(lp->gid, time, lp);
    m_new = tw_event_data(e);
    msg_set_header(test_checkpoint_magic, CLI_NEXT_OP, lp->gid, &m_new->h);    
    tw_event_send(e);
    
    return;
}

void complete_random_traffic_rc(
    struct test_checkpoint_state * ns,
    tw_bf * b,
    struct test_checkpoint_msg * msg,
    tw_lp * lp)
{
    ns->syn_data_sz -= msg->payload_sz;
}

void complete_random_traffic(
    struct test_checkpoint_state * ns,
    tw_bf * b,
    struct test_checkpoint_msg * msg,
    tw_lp * lp)
{
    assert(ns->app_id != -1);
   /* Record in some statistics? */ 
    ns->syn_data_sz += msg->payload_sz;
}
void finish_bckgnd_traffic_rc(
    struct test_checkpoint_state * ns,
    tw_bf * b,
    struct test_checkpoint_msg * msg,
    tw_lp * lp)
{
        ns->num_bursts--;
        ns->is_finished = 0;

        if(b->c0)
        {
            ns->gen_data_sz = msg->saved_data_sz;
            kickoff_synthetic_traffic_rc(ns, lp);
        }
        return;
}
void finish_bckgnd_traffic(
    struct test_checkpoint_state * ns,
    tw_bf * b,
    struct test_checkpoint_msg * msg,
    tw_lp * lp)
{
        if(ns->num_bursts > total_checkpoints || ns->num_bursts < 0)
        {
            tw_lp_suspend(lp, 0, 0);
            return;
        }
        ns->num_bursts++;
        ns->is_finished = 1;

        dprintf("\n LP %llu completed sending data %lld completed at time %lf ", lp->gid, ns->gen_data_sz, tw_now(lp));

        if(ns->num_bursts < total_checkpoints)
        {
            b->c0 = 1;
            msg->saved_data_sz = ns->gen_data_sz;
            ns->gen_data_sz = 0;
            kickoff_synthetic_traffic(ns, lp);
        }
       return;
}

void finish_nbr_wkld_rc(
    struct test_checkpoint_state * ns,
    tw_bf * b,
    struct test_checkpoint_msg * msg,
    tw_lp * lp)
{
    ns->neighbor_completed = 0;
    
    notify_neighbor_rc(ns, lp, b);
}

void finish_nbr_wkld(
    struct test_checkpoint_state * ns,
    tw_bf * b,
    struct test_checkpoint_msg * msg,
    tw_lp * lp)
{
    ns->neighbor_completed = 1;

    notify_neighbor(ns, lp, b, msg);
}

void generate_random_traffic_rc(
      struct test_checkpoint_state * ns,
      tw_bf * b,
      struct test_checkpoint_msg * msg,
      tw_lp * lp)
{
    if(b->c8)
        return;

    tw_rand_reverse_unif(lp->rng);
    tw_rand_reverse_unif(lp->rng);
    
//    model_net_event_rc2(cli_dfly_id, lp, PAYLOAD_SZ);
    model_net_event_rc2(lp, &msg->event_rc);
    ns->gen_data_sz -= PAYLOAD_SZ;
}

void generate_random_traffic(
    struct test_checkpoint_state * ns,
    tw_bf * b,
    struct test_checkpoint_msg * msg,
    tw_lp * lp)
{
    if(ns->is_finished == 1 || ns->gen_data_sz >= max_gen_data) 
    {
        b->c8 = 1;
        return;
    }
//   printf("\n LP %ld: completed %ld messages ", lp->gid, ns->num_random_pckts);
    tw_lpid global_dest_id; 
   /* Get job information */
   struct codes_jobmap_id jid; 
   jid = codes_jobmap_to_local_id(ns->cli_rel_id, jobmap_ctx); 

   int dest_svr = tw_rand_integer(lp->rng, 0, num_syn_clients - 1);
   if(dest_svr == ns->local_rank)
       dest_svr = (dest_svr + 1) % num_syn_clients;

   struct test_checkpoint_msg * m_remote = malloc(sizeof(struct test_checkpoint_msg));
   msg_set_header(test_checkpoint_magic, CLI_BCKGND_COMPLETE, lp->gid, &(m_remote->h));

   codes_mapping_get_lp_info(lp->gid, lp_grp_name, &mapping_gid, lp_name, &mapping_tid, NULL, &mapping_rid, &mapping_offset);

   jid.rank = dest_svr;
   int intm_dest_id = codes_jobmap_to_global_id(jid, jobmap_ctx);

   global_dest_id = codes_mapping_get_lpid_from_relative(intm_dest_id, NULL, CHK_LP_NM, NULL, 0);

   m_remote->payload_sz = PAYLOAD_SZ;
   ns->gen_data_sz += PAYLOAD_SZ;

   msg->event_rc  = model_net_event(cli_dfly_id, "synthetic-tr", global_dest_id, PAYLOAD_SZ, 0.0, 
           sizeof(struct test_checkpoint_msg), (const void*)m_remote, 
           0, NULL, lp);

   /* New event after MEAN_INTERVAL */
    tw_stime ts = MEAN_INTERVAL + tw_rand_unif(lp->rng); 
    tw_event * e;
    struct test_checkpoint_msg * m_new;
    e = tw_event_new(lp->gid, ts, lp);
    m_new = tw_event_data(e);
    msg_set_header(test_checkpoint_magic, CLI_BCKGND_GEN, lp->gid, &(m_new->h));    
    tw_event_send(e);
}

static void next_checkpoint_op(
        struct test_checkpoint_state * ns,
        tw_bf * bf,
        struct test_checkpoint_msg * msg,
        tw_lp * lp)
{
    assert(ns->app_id != -1);

    if(ns->op_status_ct > 0)
    {
        char buf[64];
        int written = sprintf(buf, "I/O workload operator error: %"PRIu64"  \n",lp->gid);

        lp_io_write(lp->gid, "errors %d %s ", written, buf);
        return;
    }

    ns->op_status_ct++;

    struct codes_workload_op * op_rc = malloc(sizeof(struct codes_workload_op));
    codes_workload_get_next(ns->wkld_id, 0, ns->cli_rel_id, op_rc);

    msg->saved_op_type = op_rc->op_type;
    if(op_rc->op_type == CODES_WK_END)
    {
	    ns->completion_time = tw_now(lp);
        ns->is_finished = 1; 
        
        /* Notify ranks from other job that checkpoint traffic has completed */
        int num_jobs = codes_jobmap_get_num_jobs(jobmap_ctx); 
        if(num_jobs <= 1)
        {
            bf->c9 = 1;
            return;
        }
        /* Now if notification has been received from the previous rank that
         * checkpoint has completed then we send a notification to the next
         * rank */
         notify_neighbor(ns, lp, bf, msg);
         
         if(ns->cli_rel_id == TRACK)
            tw_output(lp, "Client rank %d completed workload.\n", ns->cli_rel_id);
		
        return;
    }
    switch(op_rc->op_type)
   {
	case CODES_WK_BARRIER:
	{
        if(ns->cli_rel_id == TRACK)
		   tw_output(lp, "Client rank %d hit barrier.\n", ns->cli_rel_id);
		
        handle_next_operation(ns, lp, codes_local_latency(lp));
	}
    break;

	case CODES_WK_DELAY:
	{
        if(ns->cli_rel_id == TRACK)
		    tw_output(lp, "Client rank %d will delay for %lf seconds.\n", ns->cli_rel_id,
                op_rc->u.delay.seconds);
        tw_stime nano_secs = s_to_ns(op_rc->u.delay.seconds);
        handle_next_operation(ns, lp, nano_secs);
	}
	break;

    case CODES_WK_OPEN:
	{
        if(ns->cli_rel_id == TRACK)
		   tw_output(lp, "Client rank %d will open file id %llu \n ", ns->cli_rel_id,
		    op_rc->u.open.file_id);
		handle_next_operation(ns, lp, codes_local_latency(lp));
	}
	break;
	
	case CODES_WK_CLOSE:
	{	
        if(ns->cli_rel_id == TRACK)
		    tw_output(lp, "Client rank %d will close file id %llu \n ", ns->cli_rel_id,
                    op_rc->u.close.file_id);
		handle_next_operation(ns, lp, codes_local_latency(lp));
	}
	break;
	case CODES_WK_WRITE:
	{
        if(ns->cli_rel_id == TRACK)
		    tw_output(lp, "Client rank %d initiate write operation size %ld offset %ld .\n", ns->cli_rel_id, 
	            op_rc->u.write.size, op_rc->u.write.offset);
		send_req_to_store(ns, lp, bf, op_rc, msg, 1);
    }	
	break;

	case CODES_WK_READ:
	{
        if(ns->cli_rel_id == TRACK)
		    tw_output(lp, "Client rank %d initiate read operation size %ld offset %ld .\n", ns->cli_rel_id, op_rc->u.read.size, op_rc->u.read.offset);
                ns->num_sent_rd++;
		send_req_to_store(ns, lp, bf, op_rc, msg, 0);
    }
	break;

	default:
	      printf("\n Unknown client operation %d ", op_rc->op_type);
      }
    free(op_rc);
	
}
static void next_checkpoint_op_rc(
        struct test_checkpoint_state *ns,
        tw_bf * bf,
        struct test_checkpoint_msg *m,
        tw_lp *lp)
{
    ns->op_status_ct--;
    codes_workload_get_next_rc2(ns->wkld_id, 0, ns->cli_rel_id);

    if(m->saved_op_type == CODES_WK_END)
    {
        ns->is_finished = 0; 
       
        if(bf->c9)
            return;

        notify_neighbor_rc(ns, lp, bf);
        return;
    }
    switch(m->saved_op_type) 
    {
      case CODES_WK_READ:
      {
	    ns->num_sent_rd--;
        send_req_to_store_rc(ns, lp, bf, m);
      }
      break;

      case CODES_WK_WRITE:
      {
         ns->num_sent_wr--;
         send_req_to_store_rc(ns, lp, bf, m);
      }
      break;

      case CODES_WK_DELAY:
      {
         handle_next_operation_rc(ns, lp); 
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
	printf("\n Unknown client operation reverse %d", m->saved_op_type);
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
    if(m->h.magic != test_checkpoint_magic)
        printf("\n Msg magic %d checkpoint magic %d event-type %d src %"PRIu64" ", m->h.magic,
               test_checkpoint_magic,
               m->h.event_type,
               m->h.src);

    assert(m->h.magic == test_checkpoint_magic);

    switch(m->h.event_type) {
	case CLI_NEXT_OP:
		next_checkpoint_op(ns, b, m, lp);
	break;

	case CLI_ACK:
        ns->num_completed_ops++;
		handle_next_operation(ns, lp, codes_local_latency(lp));
	    m->saved_write_time = ns->total_write_time;
        ns->total_write_time += (tw_now(lp) - ns->write_start_time);
    break;

    case CLI_BCKGND_GEN:
        generate_random_traffic(ns, b, m, lp);
        break;

    case CLI_BCKGND_COMPLETE:
        complete_random_traffic(ns, b, m, lp);
        break;

    case CLI_BCKGND_FINISH:
        finish_bckgnd_traffic(ns, b, m, lp);
        break;

     case CLI_WKLD_FINISH:
        finish_nbr_wkld(ns, b, m, lp);
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
		next_checkpoint_op_rc(ns, b, m, lp);
	break;
	
	case CLI_ACK:
		ns->num_completed_ops--;
        ns->total_write_time = m->saved_write_time;
        handle_next_operation_rc(ns, lp);
	break;

    case CLI_BCKGND_GEN:
        generate_random_traffic_rc(ns, b, m, lp);
        break;

   case CLI_BCKGND_COMPLETE:
        complete_random_traffic_rc(ns, b, m, lp);
        break;

    case CLI_BCKGND_FINISH:
        finish_bckgnd_traffic_rc(ns, b, m, lp);
        break;
     
    case CLI_WKLD_FINISH:
        finish_nbr_wkld(ns, b, m, lp);
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
    ns->completion_time = 0;
    ns->is_finished = 0;
    ns->num_bursts = 0;

    ns->num_reads = 0;
    ns->num_writes = 0;
    ns->read_size = 0;
    ns->write_size = 0;
    ns->syn_data_sz = 0;
    ns->gen_data_sz = 0;
    ns->random_bb_node_id = -1;
    ns->neighbor_completed = 0;

    INIT_CODES_CB_INFO(&ns->cb, struct test_checkpoint_msg, h, tag, ret);

    ns->cli_rel_id = codes_mapping_get_lp_relative_id(lp->gid, 0, 0);

   codes_mapping_get_lp_info(lp->gid, lp_grp_name, &mapping_gid, lp_name, &mapping_tid, NULL, &mapping_rid, &mapping_offset);

    struct codes_jobmap_id jid;
    jid = codes_jobmap_to_local_id(ns->cli_rel_id, jobmap_ctx);
    if(jid.job == -1)
    {
        ns->app_id = -1;
        ns->local_rank = -1;
        return;
    }

    ns->app_id = jid.job;
    ns->local_rank = jid.rank;

    assert(jid.job < MAX_JOBS);

    if(strcmp(wkld_type_per_job[jid.job], "synthetic") == 0)
    {
        //printf("\n Rank %ld GID %ld generating synthetic traffic ", ns->cli_rel_id, lp->gid);
        kickoff_synthetic_traffic(ns, lp);
    }
    else if(strcmp(wkld_type_per_job[jid.job], "checkpoint") == 0)
    {
      char* w_params = (char*)&c_params;
      ns->wkld_id = codes_workload_load("checkpoint_io_workload", w_params, 0, ns->cli_rel_id);
      ns->start_time = codes_local_latency(lp);
      handle_next_operation(ns, lp, ns->start_time);
    }
    else
      {
        fprintf(stderr, "\n Invalid job id ");
        assert(0);
      }
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
   struct codes_jobmap_id jid; 
   jid = codes_jobmap_to_local_id(ns->cli_rel_id, jobmap_ctx); 
    
   if(max_write_time < ns->total_write_time)
        max_write_time = ns->total_write_time;

    total_write_time += ns->total_write_time;

    if(tw_now(lp) - ns->start_time > max_total_time)
        max_total_time = tw_now(lp) - ns->start_time;

    total_run_time += (tw_now(lp) - ns->start_time);
    total_syn_data += ns->syn_data_sz;

   int written = 0;
   //if(!ns->cli_rel_id)
   //   written = sprintf(ns->output_buf, "# Format <LP id> <Workload type> <client id> <Bytes written> <Synthetic data Received> <Time to write bytes > <Total elapsed time>");
   
   written += sprintf(ns->output_buf + written, "%"PRId64" %s %d %llu %"PRId64" %lf %lf\n", lp->gid, wkld_type_per_job[jid.job], ns->cli_rel_id, ns->write_size, ns->syn_data_sz, ns->total_write_time, tw_now(lp) - ns->start_time);
   lp_io_write(lp->gid, "checkpoint-client-stats", written, ns->output_buf);   
}

tw_lptype test_checkpoint_lp = {
    (init_f) test_checkpoint_init,
    (pre_run_f) NULL,
    (event_f) test_checkpoint_event,
    (revent_f) test_checkpoint_event_rc,
    (commit_f) NULL,
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

    rc = configuration_get_value_int(&config, "test-checkpoint-client", "chkpoint_iters", NULL,
	   &total_checkpoints);
    assert(!rc);
    c_params.total_checkpoints = total_checkpoints;

    rc = configuration_get_value_double(&config, "test-checkpoint-client", "mtti", NULL,
	   &c_params.mtti);
    assert(!rc);
   
    num_servers =
        codes_mapping_get_lp_count(NULL, 0, CODES_STORE_LP_NAME, NULL, 1);
    num_clients = 
        codes_mapping_get_lp_count(NULL, 0, CHK_LP_NM, NULL, 1);

    c_params.nprocs = num_chk_clients;
    if(num_chk_clients)
    {
        my_checkpoint_sz = terabytes_to_megabytes(c_params.checkpoint_sz)/num_chk_clients;
    }

    clients_per_server = num_clients / num_servers;
    printf("\n Number of clients per server %d ", clients_per_server);
    if (clients_per_server == 0)
        clients_per_server = 1;
}

const tw_optdef app_opt[] = {
    TWOPT_GROUP("codes-store mock test model"),
    TWOPT_CHAR("workload-conf-file", workloads_conf_file, "Workload allocation of participating ranks "),
    TWOPT_CHAR("rank-alloc-file", alloc_file, "detailed rank allocation, which rank gets what workload "),
    TWOPT_CHAR("codes-config", conf_file_name, "Name of codes configuration file"),
    TWOPT_CHAR("lp-io-dir", lp_io_dir, "Where to place io output (unspecified -> no output"),
    TWOPT_UINT("lp-io-use-suffix", lp_io_use_suffix, "Whether to append uniq suffix to lp-io directory (default 0)"),
    TWOPT_UINT("random-bb", random_bb_nodes, "Whether use randomly selected burst buffer nodes or nearby nodes"),
    TWOPT_UINT("max-gen-data", max_gen_data, "Maximum data to generate "),
    TWOPT_UINT("payload-sz", PAYLOAD_SZ, "the payload size for uniform random traffic"),
    TWOPT_END()
};

void Usage()
{
   if(tw_ismaster())
    {
        fprintf(stderr, "\n mpirun -np n ./client-mul-wklds --sync=1/3"
                "--workload-conf-file = workload-conf-file --alloc_file = alloc-file"
                "--codes-config=codes-config-file\n");
    }
}


int main(int argc, char * argv[])
{
    int num_nets, *net_ids;
    int model_net_id;
    int simple_net_id;

    g_tw_ts_end = s_to_ns(60*60*24*365); /* one year, in nsecs */

    tw_opt_add(app_opt);
    tw_init(&argc, &argv);

    if(strlen(workloads_conf_file) == 0 || strlen(alloc_file) == 0 || !conf_file_name[0])
    {
        Usage();
        MPI_Finalize();
        return 1;
    }

    /* This file will have the names of workloads and the number of
     * ranks that should be allocated to each of those workloads */
    FILE * wkld_info_file = fopen(workloads_conf_file, "r+");
    assert(wkld_info_file);

    int i =0;
    char separator = "\n";
    while(!feof(wkld_info_file))
    {
        separator = fscanf(wkld_info_file, "%d %s", &num_jobs_per_wkld[i], wkld_type_per_job[i]);
        
        if(strcmp(wkld_type_per_job[i], "synthetic") == 0)
            num_syn_clients = num_jobs_per_wkld[i];

        if(strcmp(wkld_type_per_job[i], "checkpoint") == 0)
            num_chk_clients = num_jobs_per_wkld[i];

        if(separator != EOF)
        {
            printf("\n %d instances of workload %s ", num_jobs_per_wkld[i], wkld_type_per_job[i]);
            i++;
        }
    }
    /* loading the config file into the codes-mapping utility, giving us the
     * parsed config object in return. 
     * "config" is a global var defined by codes-mapping */
    if (configuration_load(conf_file_name, MPI_COMM_WORLD, &config)){
        fprintf(stderr, "Error loading config file %s.\n", conf_file_name);
        MPI_Finalize();
        return 1;
    }

    /* Now assign the alloc file to job map */
    jobmap_p.alloc_file = alloc_file;
    jobmap_ctx = codes_jobmap_configure(CODES_JOBMAP_LIST, &jobmap_p);

    // register lps
    lsm_register();
    codes_store_register();
    resource_lp_init();
    test_checkpoint_register();
    test_dummy_register();
    codes_ex_store_register();
    model_net_register();

    /* model-net enable sampling */
//    model_net_enable_sampling(500000000, 1500000000000);

    /* Setup takes the global config object, the registered LPs, and 
     * generates/places the LPs as specified in the configuration file. 
     * This should only be called after ALL LP types have been registered in 
     * codes */
    codes_mapping_setup();

    /* Setup the model-net parameters specified in the global config object,
     * returned is the identifier for the network type */
    net_ids = model_net_configure(&num_nets);
    
    /* Network topology supported is either only simplenet
     * OR dragonfly */
    model_net_id = net_ids[0];
 
    if(net_ids[0] == DRAGONFLY_CUSTOM)
       simple_net_id = net_ids[2];
    
    free(net_ids);

    /* after the mapping configuration is loaded, let LPs parse the
     * configuration information. This is done so that LPs have access to
     * the codes_mapping interface for getting LP counts and such */
    codes_store_configure(model_net_id);
    
    if(num_nets > 1)
       codes_store_set_scnd_net(simple_net_id);
   
    resource_lp_configure();
    lsm_configure();
    test_checkpoint_configure(model_net_id);

    if(!g_tw_mynode)
    {
        char meta_file[64];
        sprintf(meta_file, "checkpoint-client-stats.meta");
        
        FILE * fp = fopen(meta_file, "w+");
        fprintf(fp, "# Format <LP id> <Workload type> <client id> <Bytes written> <Synthetic data received> <Time to write bytes> <Total time elapsed>");
        fclose(fp);

        printf("\n My checkpoint sz %lf MiB", my_checkpoint_sz);
    }

    if (lp_io_dir[0]){
        do_lp_io = 1;
        /* initialize lp io */
        int flags = lp_io_use_suffix ? LP_IO_UNIQ_SUFFIX : 0;
        int ret = lp_io_prepare(lp_io_dir, flags, &io_handle, MPI_COMM_WORLD);
        assert(ret == 0 || !"lp_io_prepare failure");
    }

    tw_run();

    double g_max_total_time, g_max_write_time, g_avg_time, g_avg_write_time, g_total_syn_data;
    MPI_Reduce(&max_total_time, &g_max_total_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);  
    MPI_Reduce(&max_write_time, &g_max_write_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);  
    MPI_Reduce(&total_write_time, &g_avg_write_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);  
    MPI_Reduce(&total_syn_data, &g_total_syn_data, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);  
    MPI_Reduce(&total_run_time, &g_avg_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);  
  
    if(!g_tw_mynode)
    {
        printf("\n Maximum time spent by compute nodes %lf Maximum time spent in write operations %lf", g_max_total_time, g_max_write_time);
        printf("\n Avg time spent by compute nodes %lf Avg time spent in write operations %lf\n", g_avg_time/num_clients, g_avg_write_time/num_chk_clients);

        printf("\n Synthetic traffic stats: data received per proc %lf ", g_total_syn_data/num_syn_clients);
    }
    if (do_lp_io){
        int ret = lp_io_flush(io_handle, MPI_COMM_WORLD);
        assert(ret == 0 || !"lp_io_flush failure");
    }

    model_net_report_stats(model_net_id);
    codes_jobmap_destroy(jobmap_ctx);
    tw_end();
    return 0;
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
