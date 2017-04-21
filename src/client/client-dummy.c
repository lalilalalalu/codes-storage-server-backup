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

#define DUMMY_LP_NM "test-dummy"
#define dprintf(_fmt, ...) \
    do {if (CLIENT_DBG) printf(_fmt, __VA_ARGS__);} while (0)

void test_dummy_register(void);

unsigned int test_dummy_magic = 0;

struct test_dummy_state
{
    int dummy_rel_id;
};

struct test_dummy_msg
{
    msg_header h;
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

static void test_dummy_event(
        struct test_dummy_state * ns,
        tw_bf * b,
        struct test_dummy_msg * m,
        tw_lp * lp)
{
    tw_error(TW_LOC, "\n Dummy LP is not intended for receiving events ");
}
static void test_dummy_event_rc(
        struct test_dummy_state * ns,
        tw_bf * b,
        struct test_dummy_msg * m,
        tw_lp * lp)
{
}

static void test_dummy_init(
        struct test_dummy_state * ns,
        tw_lp * lp)
{
    //printf("\n Dummy LP ID %llu ", lp->gid);
}

static void test_dummy_finalize(
        struct test_dummy_state *ns,
        tw_lp *lp)
{
}

tw_lptype test_dummy_lp = {
    (init_f) test_dummy_init,
    (pre_run_f) NULL,
    (event_f) test_dummy_event,
    (revent_f) test_dummy_event_rc,
    (commit_f) NULL,
    (final_f) test_dummy_finalize,
    (map_f) codes_mapping,
    sizeof(struct test_dummy_state),
};

void test_dummy_register(){
    lp_type_register(DUMMY_LP_NM, &test_dummy_lp);
}

void test_dummy_configure(int model_net_id){
    uint32_t h1=0, h2=0;

    bj_hashlittle2(DUMMY_LP_NM, strlen(DUMMY_LP_NM), &h1, &h2);
    test_dummy_magic = h1+h2;
}

