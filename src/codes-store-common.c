/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <ross.h>
#include <codes/codes_mapping.h>

#include <codes/codes-store-common.h>

char const * const CODES_STORE_LP_NAME = "codes-store";

void codes_store_init_req(
        enum codes_store_req_type type,
        int dest_rel_id,
        uint64_t oid,
        uint64_t xfer_offset,
        uint64_t xfer_size,
        struct codes_store_request *req)
{
    req->type = type;
    req->dest_rel_id = dest_rel_id;
    req->oid = oid;
    req->xfer_offset = xfer_offset;
    req->xfer_size = xfer_size;
}

tw_lpid codes_store_get_store_lpid(
        int rel_id,
        char const * annotation,
        bool ignore_annotations)
{
    return codes_mapping_get_lpid_from_relative(rel_id, NULL,
            CODES_STORE_LP_NAME, annotation, ignore_annotations);
}

tw_lpid codes_store_get_local_store_lpid(
        tw_lp const * lp,
        bool ignore_annotations)
{
    char group_name[MAX_NAME_LENGTH];
    char anno[MAX_NAME_LENGTH];
    int rep_id, dummy;
    tw_lpid rtn;

    codes_mapping_get_lp_info(lp->gid, group_name, NULL, NULL,
            NULL, ignore_annotations ? NULL : anno, &rep_id, &dummy);

    codes_mapping_get_lp_id(group_name, CODES_STORE_LP_NAME, anno,
            ignore_annotations, rep_id, 0, &rtn);

    return rtn;
}
