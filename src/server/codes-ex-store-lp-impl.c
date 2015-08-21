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

/* the external store LP only keeps track of the number of bytes written */
typedef struct es_state_s {
	/* number of total bytes written on external store */
	int bytes_written;
	/* output buffer */
	char output_buf[256];
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
    ns->bytes_written -= m->xfer_size;
}

/* writing to the store through model-net event */
static void handle_write_to_store(
	    es_state_t * ns,
	    tw_bf * b,
	    es_msg * m,
	    tw_lp * lp)
{
    ns->bytes_written += m->xfer_size;

    return;
}

void codes_ex_store_register(void)
{
    uint32_t h1=0, h2=0;

    bj_hashlittle2("codes-external-store", strlen("codes-external-store"), &h1, &h2);
    ces_magic = h1+h2;

    lp_type_register(CODES_EX_STORE_LP_NAME, &es_lp);
}
