/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef CODES_STORE_COMMON_H
#define CODES_STORE_COMMON_H

#include <stdint.h>
#include <stdbool.h>
#include <ross.h>

extern char const * const CODES_STORE_LP_NAME;

enum codes_store_req_type {
    CSREQ_OPEN,
    CSREQ_CREATE,
    CSREQ_READ,
    CSREQ_WRITE
};

struct codes_store_request {
    enum codes_store_req_type type;
    uint64_t oid;
    uint64_t xfer_offset;
    uint64_t xfer_size;
};

void codes_store_init_req(
        enum codes_store_req_type type,
        uint64_t oid,
        uint64_t xfer_offset,
        uint64_t xfer_size,
        struct codes_store_request *req);

tw_lpid codes_store_get_store_lpid(
        int rel_id,
        char const * annotation,
        bool ignore_annotations);

tw_lpid codes_store_get_local_store_lpid(
        tw_lp const * lp,
        bool ignore_annotations);

#endif /* end of include guard: CODES_STORE_COMMON_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 *  indent-tabs-mode: nil
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
