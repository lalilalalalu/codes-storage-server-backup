/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <codes/codes-store-cli.h>
#include <codes/codes-callback.h>

int main (int argc, char * argv[])
{
    /* just dummy parameters until we have real tests to run */
    struct codes_store_request r;
    struct codes_cb_info cb = {100, 0, 0, 0};
    codes_store_send_req(&r, 0, NULL, 0, NULL, &cb);
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
