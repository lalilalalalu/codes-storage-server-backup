/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */
#include <string.h>
#include <assert.h>
#include <codes/codes_mapping.h>
#include <codes/lp-type-lookup.h>
#include <codes/jenkins-hash.h>
#include <codes/lp-io.h>
#include <codes/codes-callback.h>
#include <codes/quicklist.h>
#include <codes/codes-external-store.h>

struct es_param
{
   /* startup latency */
   double latency;
   /* bandwidth of the model */
   double bw_mbps;
};

typedef struct es_param es_param;


/* the external store LP only keeps track of the number of bytes written */
typedef struct es_state_s {
	/* latency, bandwidth parameters */
	es_param params;	
	/* number of total bytes written on external store */
	int bytes_written;
	/* output buffer */
	char output_buf[256];
	/* receive queue time */
	tw_stime * next_recv_idle_time;
       /* number of connected BB or IO forwarding nodes*/
	int num_io_nodes;
} es_state_t;

static void es_init(es_state_t * ns, tw_lp * lp);
static void es_event_handler(
	    es_state_t * ns,
	    tw_bf * b,
	    es_msg * m,
	    tw_lp * lp);
static void es_event_handler_rc(
	    es_state_t * ns,
	    tw_bf * b,
	    es_msg * m,
	    tw_lp * lp);
static void es_finalize(es_state_t * ns, tw_lp * lp);
static void handle_write_to_store_rc(
	    es_state_t * ns,
	    tw_bf * b,
	    es_msg * m,
	    tw_lp * lp);
static void handle_write_to_store(
	    es_state_t * ns,
	    tw_bf * b,
	    es_msg * m,
	    tw_lp * lp);
static void handle_write_completion(
	    es_state_t * ns,
	    tw_bf * b,
	    es_msg * m,
	    tw_lp * lp);
static void handle_write_completion_rc(
	    es_state_t * ns,
	    tw_bf * b,
	    es_msg * m,
	    tw_lp * lp);
static tw_stime rate_to_ns(uint64_t bytes, double MB_p_s);
///// END LP, EVENT PROCESSING FUNCTION DECLS /////

///// BEGIN SIMULATION DATA STRUCTURES /////

tw_lptype es_lp = {
    (init_f) es_init,
    (pre_run_f) NULL,
    (event_f) es_event_handler,
    (revent_f) es_event_handler_rc,
    (final_f) es_finalize,
    (map_f) codes_mapping,
    sizeof(es_state_t),
};

static void es_init(es_state_t * ns, tw_lp * lp)
{
    int i;
    uint32_t h1=0, h2=0;

    bj_hashlittle2(CODES_EX_STORE_LP_NAME, strlen(CODES_EX_STORE_LP_NAME), &h1, &h2);
    ces_magic = h1+h2;
   
    int rc; 
    rc = configuration_get_value_double(&config, CODES_EX_STORE_LP_NAME,
            "startup_ns", NULL, &ns->params.latency);
    if( rc < 0)
	ns->params.latency = 10000; /* default to 10 us */
    rc = configuration_get_value_double(&config, CODES_EX_STORE_LP_NAME,
            "bw_mbps", NULL, &ns->params.bw_mbps);
    
    if(rc < 0)
       ns->params.bw_mbps = 16000; /* Setting default to 16 Gbps */
    
   rc = configuration_get_value_int(&config, CODES_EX_STORE_LP_NAME,
            "num_io_nodes", NULL, &ns->num_io_nodes);

   if(rc < 0)
	tw_error(TW_LOC, "\n Number of I/O nodes must be specified ");

   ns->next_recv_idle_time = malloc(ns->num_io_nodes * sizeof(tw_stime));
   tw_stime st = tw_now(lp);
   for( i = 0; i < ns->num_io_nodes; i++)
	ns->next_recv_idle_time[i] = st;
}

static void es_event_handler(
	    es_state_t * ns,
	    tw_bf * b,
	    es_msg * m,
	    tw_lp * lp)
{
    assert(m->h.magic == ces_magic);

    switch(m->h.event_type)
    {
        case CES_WRITE:
		handle_write_to_store(ns, b, m, lp);
	break;

	case CES_WRITE_COMPLETE:
		handle_write_completion(ns, b, m, lp);
	break;

	default:
		tw_error(TW_LOC, "unknown es event type ");
    }
}

static void es_event_handler_rc(
	    es_state_t * ns,
	    tw_bf * b,
	    es_msg * m,
	    tw_lp * lp)
{
    assert(m->h.magic == ces_magic);

    switch(m->h.event_type)
    {
        case CES_WRITE:
		handle_write_to_store_rc(ns, b, m, lp);
	break;

	case CES_WRITE_COMPLETE:
		handle_write_completion_rc(ns, b, m, lp);
	break;
	default:
		tw_error(TW_LOC, "unknown es event type ");
    }
}

static void es_finalize(es_state_t * ns, tw_lp * lp)
{
    char data[1024];
    char id[32];
    sprintf(data, "lp:%ld\tbytes_written:%d\n", lp->gid, ns->bytes_written);
    lp_io_write(lp->gid, "es-stats", strlen(data), data);
}

static void handle_write_to_store_rc(
	    es_state_t * ns,
	    tw_bf * b,
	    es_msg * m,
	    tw_lp * lp)
{
    tw_lpid global_node_id = m->h.src;
    int local_node_id = codes_mapping_get_lp_relative_id(global_node_id, 0, 0);
    ns->next_recv_idle_time[local_node_id] = m->saved_idle_time;
}

/* writing to the store through model-net event */
static void handle_write_to_store(
	    es_state_t * ns,
	    tw_bf * b,
	    es_msg * m,
	    tw_lp * lp)
{
    ns->bytes_written += m->num_bytes;

    tw_lpid global_node_id = m->h.src;
    int local_node_id = codes_mapping_get_lp_relative_id(global_node_id, 0, 0);
    // TODO: Add self suspend mode
    //assert(local_node_id >= 0 && local_node_id < ns->num_io_nodes);
    
    tw_stime queue_time = 0;
  
    m->saved_idle_time = ns->next_recv_idle_time[local_node_id];
    if(ns->next_recv_idle_time[local_node_id] > tw_now(lp))
	queue_time = ns->next_recv_idle_time[local_node_id] - tw_now(lp);
   
    queue_time += rate_to_ns(m->num_bytes, ns->params.bw_mbps); 
    ns->next_recv_idle_time[local_node_id] = tw_now(lp) + queue_time;

    /* Issue completion event */
    tw_event * e;
    es_msg * m_out; 
    e = codes_event_new(lp->gid, queue_time, lp);
    m_out = (es_msg*)tw_event_data(e);

    memcpy(m_out, m, sizeof(es_msg) + m->self_event_size); 
    msg_set_header(ces_magic, CES_WRITE_COMPLETE, m->h.src, &m_out->h);

    tw_event_send(e);
 
    return;
}

static void handle_write_completion_rc(
	    es_state_t * ns,
	    tw_bf * b,
	    es_msg * m,
	    tw_lp * lp)
{
    return;
}

static void handle_write_completion(
	    es_state_t * ns,
	    tw_bf * b,
	    es_msg * m,
	    tw_lp * lp)
{
   /* send I/O completion notification to sender */
    tw_event * e;
    void* m_out;
    void * tmp_ptr = m + 1;

    e = codes_event_new(m->h.src, codes_local_latency(lp), lp);  
    m_out = tw_event_data(e);

    memcpy(m_out, tmp_ptr, m->self_event_size);

    tw_event_send(e);

}

/* convert MiB/s and bytes to ns */
static tw_stime rate_to_ns(uint64_t bytes, double MB_p_s)
{
    tw_stime time;

    /* bytes to MB */
    time = ((double)bytes)/(1024.0*1024.0);
    /* MB to s */
    time = time / MB_p_s;
    /* s to ns */
    time = time * 1000.0 * 1000.0 * 1000.0;

    return(time);
}

void codes_ex_store_register(void)
{
    uint32_t h1=0, h2=0;

    bj_hashlittle2("codes-external-store", strlen("codes-external-store"), &h1, &h2);
    ces_magic = h1+h2;

    lp_type_register(CODES_EX_STORE_LP_NAME, &es_lp);
}
