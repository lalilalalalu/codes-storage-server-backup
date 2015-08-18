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

#include "../codes/codes-store-lp.h"
#include "test-client.h"

#define CLIENT_LP_NM "test-client"

#define CLIENT_DBG 0
#define dprintf(_fmt, ...) \
    do {if (CLIENT_DBG) printf(_fmt, __VA_ARGS__);} while (0)

static int num_reqs;
static int req_size;
static int test_client_magic;
static int cli_mn_id;
// following is for mapping clients to servers
static int do_server_mapping = 0;
static int num_servers;
static int num_clients;
static int clients_per_server;

enum test_client_event
{
    TEST_CLI_ACK = 12,
};

struct test_client_state
{
    int cli_rel_id;
    int num_complete_wr;
    int num_complete_rd;
    struct codes_cb_info cb;
};

struct test_client_msg
{
    msg_header h;
    int tag;
    codes_store_ret_t ret;
};

static void next(
        int is_write,
        struct test_client_state * ns,
        struct test_client_msg * m,
        tw_lp * lp)
{
    struct codes_store_request r;
    msg_header h;

    int n = is_write ? ns->num_complete_wr : ns->num_complete_rd;

    codes_store_init_req(
            is_write? CSREQ_WRITE:CSREQ_READ, 0, 0, n*req_size, req_size, &r);

    msg_set_header(test_client_magic, TEST_CLI_ACK, lp->gid, &h);

    int dest_server_id;
    if (do_server_mapping)
        dest_server_id = ns->cli_rel_id / clients_per_server;
    else
        dest_server_id = 0;
    codes_store_send_req(&r, dest_server_id, lp, cli_mn_id, CODES_MCTX_DEFAULT,
            0, &h, &ns->cb);

//    printf("\n Client %d sending to model-net ID %d ", lp->gid, lp->gid + 3);
    
    dprintf("%lu: sent %s request\n", lp->gid, is_write ? "write" : "read");
}
static void next_rc(
        int is_write,
        struct test_client_state *ns,
        struct test_client_msg *m,
        tw_lp *lp)
{
    codes_store_send_req_rc(cli_mn_id, lp);
    dprintf("%lu: sent %s request (rc)\n", lp->gid, is_write ? "write" : "read");
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
            dprintf("%lu: received ack\n", lp->gid);
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
            dprintf("%lu: received ack (rc)\n", lp->gid);
            assert(m->ret == CODES_STORE_OK);
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
    INIT_CODES_CB_INFO(&ns->cb, struct test_client_msg, h, tag, ret);

    ns->cli_rel_id = codes_mapping_get_lp_relative_id(lp->gid, 0, 0);
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
    cli_mn_id = model_net_id;

    int rc;
    rc = configuration_get_value_int(&config, "test-client", "num_reqs", NULL,
            &num_reqs);
    assert(!rc);
    rc = configuration_get_value_int(&config, "test-client", "req_size", NULL,
            &req_size);
    assert(!rc);

    configuration_get_value_int(&config, "test-client", "do_server_mapping", NULL,
            &do_server_mapping);

    num_servers =
        codes_mapping_get_lp_count(NULL, 0, CODES_STORE_LP_NAME, NULL, 1);
    num_clients =
        codes_mapping_get_lp_count(NULL, 0, CLIENT_LP_NM, NULL, 1);
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
