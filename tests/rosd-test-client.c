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

#include "../src/util/msg.h"

#define CLIENT_LP_NM "test-client"

static int num_reqs;
static int req_size;
static int test_client_magic;
static int cli_mn_id;

enum test_client_event
{
    TEST_CLI_ACK = 12,
};

struct test_client_state
{
    int num_complete_wr;
    int num_complete_rd;
};

struct test_client_msg
{
    msg_header h;
    triton_io_gresp g;
};

static void next(
        int is_write,
        struct test_client_state * ns,
        struct test_client_msg * m,
        tw_lp * lp)
{
    triton_io_greq r;
    int n = is_write ? ns->num_complete_wr : ns->num_complete_rd;

    r.req.req_type = is_write ? REQ_WRITE : REQ_READ;
    r.req.create = 0;
    r.req.oid = 0;
    r.req.xfer_offset = n * req_size;
    r.req.xfer_size = req_size;

    msg_set_header(test_client_magic, TEST_CLI_ACK, lp->gid,
            &r.callback.header);
    r.callback.op_index = 0;
    r.callback.event_size = sizeof(struct test_client_msg);
    r.callback.header_offset = offsetof(struct test_client_msg, h);
    r.callback.resp_offset   = offsetof(struct test_client_msg, g);

    triton_send_request(&r, lp, cli_mn_id);

    printf("%lu: sent %s request\n", lp->gid, is_write ? "write" : "read");
}
static void next_rc(
        int is_write,
        struct test_client_state *ns,
        struct test_client_msg *m,
        tw_lp *lp)
{
    triton_send_request_rev(req_size, lp, cli_mn_id);
    printf("%lu: sent %s request (rc)\n", lp->gid, is_write ? "write" : "read");
}


static void test_client_event(
        struct test_client_state * ns,
        tw_bf * b,
        struct test_client_msg * m,
        tw_lp * lp)
{
    assert(m->h.magic == test_client_magic);

    switch(m->h.event_type) {
        case TEST_CLI_ACK:
            printf("%lu: received ack\n", lp->gid);
            if (ns->num_complete_wr == num_reqs) {
                b->c0 = 1;
                ns->num_complete_rd++;
                if (ns->num_complete_rd < num_reqs)
                    next(0, ns, m, lp);
            }
            else {
                ns->num_complete_wr++;
                b->c1 = ns->num_complete_wr < num_reqs;
                next(b->c1, ns, m, lp);
            }
            break;
        default:
            assert(0);
    }
}
static void test_client_event_rc(
        struct test_client_state * ns,
        tw_bf * b,
        struct test_client_msg * m,
        tw_lp * lp)
{
    assert(m->h.magic == test_client_magic);

    switch(m->h.event_type) {
        case TEST_CLI_ACK:
            printf("%lu: received ack (rc)\n", lp->gid);
            if (b->c0) {
                next_rc(0, ns, m, lp);
                ns->num_complete_rd--;
            }
            else {
                next_rc(b->c1, ns, m, lp);
                ns->num_complete_wr--;
            }
            break;
        default:
            assert(0);
    }
}

static void test_client_init(
        struct test_client_state * ns,
        tw_lp * lp)
{
    ns->num_complete_wr = 0;
    ns->num_complete_rd = 0;
}

static void test_client_pre_run(
        struct test_client_state *ns,
        tw_lp *lp)
{
    next(1, ns, NULL, lp);
}

static void test_client_finalize(
        struct test_client_state *ns,
        tw_lp *lp)
{
    if (ns->num_complete_wr != num_reqs)
        tw_error(TW_LOC, "num_complete_wr:%d does not match num_reqs:%d\n",
                ns->num_complete_wr, num_reqs);
    if (ns->num_complete_rd != num_reqs)
        tw_error(TW_LOC, "num_complete_rd:%d does not match num_reqs:%d\n",
                ns->num_complete_rd, num_reqs);
}

tw_lptype test_client_lp = {
    (init_f) test_client_init,
    (pre_run_f) test_client_pre_run,
    (event_f) test_client_event,
    (revent_f) test_client_event_rc,
    (final_f) test_client_finalize,
    (map_f) codes_mapping,
    sizeof(struct test_client_state),
};

void test_client_register(){
    lp_type_register(CLIENT_LP_NM, &test_client_lp);
}

void test_client_configure(int model_net_id){
    uint32_t h1=0, h2=0;

    bj_hashlittle2(CLIENT_LP_NM, strlen(CLIENT_LP_NM), &h1, &h2);
    test_client_magic = h1+h2;

    int rc;
    rc = configuration_get_value_int(&config, "test-client", "num_reqs", NULL,
            &num_reqs);
    assert(!rc);
    rc = configuration_get_value_int(&config, "test-client", "req_size", NULL,
            &req_size);
    assert(!rc);
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
