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
#include "../codes/codes-store-lp.h"

#define CHK_LP_NM "test-checkpoint-client"
#define MEAN_INTERVAL 1000000.0
#define CLIENT_DBG 1
#define MAX_PAYLOAD_SZ 2048
#define TRACK 0
#define MAX_JOBS 5
#define GENERATE_TRAFFIC 1
#define dprintf(_fmt, ...) \
    do {if (CLIENT_DBG) printf(_fmt, __VA_ARGS__);} while (0)

void test_checkpoint_register(void);

/* configures the lp given the global config object */
void test_checkpoint_configure(int model_net_id);

static char lp_io_dir[256] = {'\0'};
static unsigned int lp_io_use_suffix = 0;
static int do_lp_io = 0;
static lp_io_handle io_handle;

static char workloads_conf_file[4096] = {'\0'};
static char alloc_file[4096] = {'\0'};
static char conf_file_name[4096] = {'\0'};

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
static int num_nw_lps;

// following is for mapping clients to servers
static int num_servers;
static int num_clients;
static int clients_per_server;
static double my_checkpoint_sz;

static checkpoint_wrkld_params c_params = {0, 0, 0, 0, 0};

static tw_stime ns_to_s(tw_stime ns);
static tw_stime s_to_ns(tw_stime ns);

enum test_checkpoint_event
{
    CLI_NEXT_OP=12,
    CLI_ACK,
    CLI_BCKGND_GEN,
    CLI_BCKGND_COMPLETE
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
    int64_t num_random_pckts;

    /* For running multiple workloads */
    int app_id;
    int local_rank;
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

static double terabytes_to_megabytes(double tib)
{
    return (tib * 1000.0 * 1000.0);
}

static void kickoff_synthetic_traffic(
        struct test_checkpoint_state * ns,
        tw_lp * lp)
{
        /* Generate random traffic during the delay */
        tw_event * e;
        struct test_checkpoint_msg * m_new;
        tw_stime ts = (1.1 * g_tw_lookahead) + tw_rand_exponential(lp->rng, MEAN_INTERVAL);
        e = codes_event_new(lp->gid, ts, lp);
        m_new = tw_event_data(e);
        msg_set_header(test_checkpoint_magic, CLI_BCKGND_GEN, lp->gid, &m_new->h);    
        tw_event_send(e);
}

static void send_req_to_store_rc(
	struct test_checkpoint_state * ns,
        tw_lp * lp,
        struct test_checkpoint_msg * m)
{
	codes_store_send_req_rc(cli_dfly_id, lp);	
    ns->write_size -= m->op_rc.u.write.size;
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
        dprintf("%llu: sent %s request\n", lp->gid, is_write ? "write" : "read");

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

void complete_random_traffic_rc(
    struct test_checkpoint_state * ns,
    tw_bf * b,
    struct test_checkpoint_msg * msg,
    tw_lp * lp)
{
    ns->num_random_pckts--;
}

void complete_random_traffic(
    struct test_checkpoint_state * ns,
    tw_bf * b,
    struct test_checkpoint_msg * msg,
    tw_lp * lp)
{
   /* Record in some statistics? */ 
    ns->num_random_pckts++;
    if(lp->gid == TRACK)
        dprintf("\n Packet %lld completed at time %lf ", ns->num_random_pckts, tw_now(lp));

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
        tw_rand_reverse_unif(lp->rng);

    if(b->c0)
    {
        codes_local_latency_reverse(lp); 
        handle_next_operation_rc(ns, lp);
    }
}
void generate_random_traffic(
    struct test_checkpoint_state * ns,
    tw_bf * b,
    struct test_checkpoint_msg * msg,
    tw_lp * lp)
{
   b->c0 = 0;
   b->c1 = 0;

   tw_lpid global_dest_id; 
   /* Get job information */
   struct codes_jobmap_id jid; 
   jid = codes_jobmap_to_local_id(ns->cli_rel_id, jobmap_ctx); 

   int num_clients = num_jobs_per_wkld[jid.job];
   int dest_svr = tw_rand_integer(lp->rng, 0, num_clients - 1);
   if(dest_svr == ns->cli_rel_id)
       dest_svr = (dest_svr + 1) % num_clients;

   struct test_checkpoint_msg * m_remote = malloc(sizeof(struct test_checkpoint_msg));
   msg_set_header(test_checkpoint_magic, CLI_BCKGND_COMPLETE, lp->gid, &(m_remote->h));

   codes_mapping_get_lp_info(lp->gid, lp_grp_name, &mapping_gid, lp_name, &mapping_tid, NULL, &mapping_rid, &mapping_offset);

   jid.rank = dest_svr;
   int intm_dest_id = codes_jobmap_to_global_id(jid, jobmap_ctx);

   codes_mapping_get_lp_id(lp_grp_name, lp_name, NULL, 1,
           intm_dest_id / num_nw_lps, intm_dest_id % num_nw_lps, &global_dest_id);

   printf("\n Global dest id for %ld is %ld ", dest_svr, global_dest_id);

   int payload_sz = tw_rand_integer(lp->rng, 0, MAX_PAYLOAD_SZ);
   model_net_event(cli_dfly_id, "synthetic-tr", global_dest_id, payload_sz, 0.0, 
           sizeof(struct test_checkpoint_msg), (const void*)m_remote, 
           0, NULL, lp);
   msg->payload_sz = payload_sz;

   /* New event after MEAN_INTERVAL */
/*    tw_stime ts = (1.1 * g_tw_lookahead) + tw_rand_exponential(lp->rng, MEAN_INTERVAL); 
    tw_event * e;
    struct test_checkpoint_msg * m_new;
    e = tw_event_new(lp->gid, ts, lp);
    m_new = tw_event_data(e);
    msg_set_header(test_checkpoint_magic, CLI_BCKGND_GEN, lp->gid, &(m_new->h));    
    tw_event_send(e);
*/
}

static void next_checkpoint_op(
        struct test_checkpoint_state * ns,
        tw_bf * bf,
        struct test_checkpoint_msg * msg,
        tw_lp * lp)
{
    if(ns->op_status_ct > 0)
    {
        char buf[64];
        int written = sprintf(buf, "I/O workload operator error: %llu  \n",lp->gid);

        lp_io_write(lp->gid, "errors %d %s ", written, buf);
        return;
    }

    ns->op_status_ct++;

    struct codes_workload_op op_rc;
    codes_workload_get_next(ns->wkld_id, ns->app_id, ns->cli_rel_id, &op_rc);

    /* save the op in the message */
    msg->op_rc = op_rc;
   
    if(op_rc.op_type == CODES_WK_END)
    {
		ns->completion_time = tw_now(lp);
        
//        if(lp->gid == TRACK)
            dprintf("Client rank %d completed workload.\n", ns->cli_rel_id);
		
        return;
    
    }
    switch(op_rc.op_type)
   {
	case CODES_WK_BARRIER:
	{
//        if(lp->gid == TRACK)
		    dprintf("Client rank %d hit barrier.\n", ns->cli_rel_id);
		
        handle_next_operation(ns, lp, codes_local_latency(lp));
	}
    break;

	case CODES_WK_DELAY:
	{
//        if(lp->gid == TRACK)
		    dprintf("Client rank %d will delay for %lf seconds.\n", ns->cli_rel_id,
                msg->op_rc.u.delay.seconds);
                tw_stime nano_secs = s_to_ns(msg->op_rc.u.delay.seconds);
        handle_next_operation(ns, lp, nano_secs);
	}
	break;

    case CODES_WK_OPEN:
	{
//        if(lp->gid == TRACK)
		    dprintf("Client rank %d will open file id %ld \n ", ns->cli_rel_id,
		    msg->op_rc.u.open.file_id);
		handle_next_operation(ns, lp, codes_local_latency(lp));
	}
	break;
	
	case CODES_WK_CLOSE:
	{	
//        if(lp->gid == TRACK)
		    dprintf("Client rank %d will close file id %ld \n ", ns->cli_rel_id,
                    msg->op_rc.u.close.file_id);
		handle_next_operation(ns, lp, codes_local_latency(lp));
	}
	break;
	case CODES_WK_WRITE:
	{
//        if(lp->gid == TRACK)
		    dprintf("Client rank %d initiate write operation size %ld offset %ld .\n", ns->cli_rel_id, 
		
        msg->op_rc.u.write.size, msg->op_rc.u.write.offset);
		send_req_to_store(ns, lp, msg, 1);
    }	
	break;

	case CODES_WK_READ:
	{
//        if(lp->gid == TRACK)
		    dprintf("Client rank %d initiate write operation size %ld offset %ld .\n", ns->cli_rel_id, msg->op_rc.u.read.size, msg->op_rc.u.read.offset);
                ns->num_sent_rd++;
		send_req_to_store(ns, lp, msg, 0);
    }
	break;

	default:
	      printf("\n Unknown client operation %d ", msg->op_rc.op_type);
      }
	
}
static void next_checkpoint_op_rc(
        struct test_checkpoint_state *ns,
        tw_bf * bf,
        struct test_checkpoint_msg *m,
        tw_lp *lp)
{
    ns->op_status_ct--;
    codes_workload_get_next_rc(ns->wkld_id, ns->app_id, ns->cli_rel_id, &m->op_rc);

    if(m->op_rc.op_type == CODES_WK_END)
        return;

    switch(m->op_rc.op_type) 
    {
      case CODES_WK_READ:
      {
	    ns->num_sent_rd--;
        send_req_to_store_rc(ns, lp, m);
      }
      break;

      case CODES_WK_WRITE:
      {
         ns->num_sent_wr--;
         send_req_to_store_rc(ns, lp, m);
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
    ns->num_random_pckts = 0;

    INIT_CODES_CB_INFO(&ns->cb, struct test_checkpoint_msg, h, tag, ret);

    ns->cli_rel_id = codes_mapping_get_lp_relative_id(lp->gid, 0, 0);
    ns->start_time = tw_now(lp);

   codes_mapping_get_lp_info(lp->gid, lp_grp_name, &mapping_gid, lp_name, &mapping_tid, NULL, &mapping_rid, &mapping_offset);
    num_nw_lps = codes_mapping_get_lp_count(lp_grp_name, 1,"test-checkpoint-client", NULL, 1); 

    struct codes_jobmap_id jid;
    jid = codes_jobmap_to_local_id(ns->cli_rel_id, jobmap_ctx);
    if(jid.job == -1)
    {
//        printf("\n Client %d not generating traffic ", ns->cli_rel_id);
        ns->app_id = -1;
        ns->local_rank = -1;
        return;
    }

    assert(jid.job < MAX_JOBS);

    if(strcmp(wkld_type_per_job[jid.job], "synthetic") == 0)
    {
//        printf("\n Rank %ld generating synthetic traffic ", lp->gid);
        kickoff_synthetic_traffic(ns, lp);
        ns->app_id = jid.job;
    }
    else if(strcmp(wkld_type_per_job[jid.job], "checkpoint") == 0)
    {
//      printf("\n Rank %ld generating checkpoint traffic ", lp->gid);
      char* w_params = (char*)&c_params;
      ns->wkld_id = codes_workload_load("checkpoint_io_workload", w_params, ns->app_id, ns->cli_rel_id);
      handle_next_operation(ns, lp, codes_local_latency(lp));
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
   int written = 0;
   if(!ns->cli_rel_id)
      written = sprintf(ns->output_buf, "# Format <LP id> <Bytes written> <Time to write bytes >");
   
   written += sprintf(ns->output_buf + written, "%lu %lu %ld %lf \n", lp->gid, ns->cli_rel_id, ns->write_size, ns->total_write_time);
   lp_io_write(lp->gid, "checkpoint-client-stats", written, ns->output_buf);   
}

tw_lptype test_checkpoint_lp = {
    (init_f) test_checkpoint_init,
    (pre_run_f) NULL,
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

    rc = configuration_get_value_int(&config, "test-checkpoint-client", "chkpoint_iters", NULL,
	   &c_params.total_checkpoints);
    assert(!rc);

    rc = configuration_get_value_double(&config, "test-checkpoint-client", "mtti", NULL,
	   &c_params.mtti);
    assert(!rc);
   
    num_servers =
        codes_mapping_get_lp_count(NULL, 0, CODES_STORE_LP_NAME, NULL, 1);
    num_clients =
        codes_mapping_get_lp_count(NULL, 0, CHK_LP_NM, NULL, 1);

    c_params.nprocs = num_clients;

    my_checkpoint_sz = terabytes_to_megabytes(c_params.checkpoint_sz)/num_clients;
    
    clients_per_server = num_clients / num_servers;
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
    TWOPT_END()
};

void Usage()
{
   if(tw_ismaster())
    {
        fprintf(stderr, "\n mpirun -np n ./client-mul-wklds --sync=1/3"
                "--workload_conf_file = workload-conf-file --alloc_file = alloc-file"
                "--conf=codes-config-file\n");
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
    codes_ex_store_register();
    model_net_register();

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
 
    if(net_ids[0] == DRAGONFLY)
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


    if (lp_io_dir[0]){
        do_lp_io = 1;
        /* initialize lp io */
        int flags = lp_io_use_suffix ? LP_IO_UNIQ_SUFFIX : 0;
        int ret = lp_io_prepare(lp_io_dir, flags, &io_handle, MPI_COMM_WORLD);
        assert(ret == 0 || !"lp_io_prepare failure");
    }

    tw_run();

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