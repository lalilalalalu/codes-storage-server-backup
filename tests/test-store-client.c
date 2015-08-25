/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <ross.h>
#include <codes/configuration.h>
#include <codes/local-storage-model.h>
#include <codes/model-net.h>
#include <codes/codes_mapping.h>
#include <codes/lp-io.h>
#include <codes/resource-lp.h>

#include <codes/codes-store-lp.h>
#include <codes/codes-external-store.h>
#include <codes/codes-store-client-lp.h>
#include <codes/codes-store-client-barrier-lp.h>

static tw_stime s_to_ns(tw_stime s)
{
    return(s * (1000.0 * 1000.0 * 1000.0));
}

static char conf_file_name[256] = {'\0'};
static char lp_io_dir[256] = {'\0'};
static unsigned int lp_io_use_suffix = 0;
static int do_lp_io = 0;
static lp_io_handle io_handle;

const tw_optdef app_opt[] = {
    TWOPT_GROUP("codes-store mock test model"),
    TWOPT_CHAR("codes-config", conf_file_name, "Name of codes configuration file"),
    TWOPT_CHAR("lp-io-dir", lp_io_dir, "Where to place io output (unspecified -> no output"),
    TWOPT_UINT("lp-io-use-suffix", lp_io_use_suffix, "Whether to append uniq suffix to lp-io directory (default 0)"),
    TWOPT_END()
};

int main(int argc, char * argv[])
{
    int num_nets, *net_ids;

    g_tw_ts_end = s_to_ns(60*60*24*365); /* one year, in nsecs */

    tw_opt_add(app_opt);
    tw_init(&argc, &argv);

    if (!conf_file_name[0]) {
        fprintf(stderr, "Expected \"codes-config\" option, please see --help.\n");
        MPI_Finalize();
        return 1;
    }

    /* loading the config file into the codes-mapping utility, giving us the
     * parsed config object in return. 
     * "config" is a global var defined by codes-mapping */
    if (configuration_load(conf_file_name, MPI_COMM_WORLD, &config)){
        fprintf(stderr, "Error loading config file %s.\n", conf_file_name);
        MPI_Finalize();
        return 1;
    }

    // register lps
    lsm_register();
    codes_store_register();
    resource_lp_init();
    codes_ex_store_register();
    model_net_register();
    cs_client_register();
    barrier_register();

    /* Setup takes the global config object, the registered LPs, and 
     * generates/places the LPs as specified in the configuration file. 
     * This should only be called after ALL LP types have been registered in 
     * codes */
    codes_mapping_setup();

    /* Setup the model-net parameters specified in the global config object,
     * returned is the identifier for the network type */
    net_ids = model_net_configure(&num_nets);

    /* after the mapping configuration is loaded, let LPs parse the
     * configuration information. This is done so that LPs have access to
     * the codes_mapping interface for getting LP counts and such */
    codes_store_configure(net_ids[0]);

    if(num_nets == 2)
       codes_store_set_scnd_net(net_ids[1]);

    resource_lp_configure();
    lsm_configure();
    cs_client_configure(net_ids[0]);

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

    model_net_report_stats(net_ids[0]);
    free(net_ids);
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
