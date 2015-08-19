/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include "dist.h" 
#include <string.h>


void map_logical_to_physical_objs(
        unsigned int stripe_factor, 
        unsigned int strip_size, 
        uint64_t log_off, 
        uint64_t log_len, 
        uint64_t *obj_offs,
        uint64_t *obj_lens) {
    uint64_t stripe_size = (uint64_t)strip_size*(uint64_t)stripe_factor;
    /* adjust for full stripes */
    uint64_t stripes_skip = log_off / stripe_size;
    uint64_t start_obj_off = stripes_skip * strip_size; 
    uint64_t log_off_adj  = log_off - stripes_skip*stripe_size;

    /* compute "real" strip locations for indexing */
    uint64_t start_strip  = log_off_adj / strip_size;
    uint64_t end_strip    = (log_off_adj+log_len) / strip_size;

    /* adjust offset so that we can consider it as hitting the "first" object 
     * readjustments are made through indexing */
    log_off_adj -= (log_off_adj / strip_size) * strip_size;

    /* to make things simpler, we start off at an exact strip boundary then
     * readjust at the end */
    uint64_t rem = log_len + log_off_adj;
    unsigned int svr;
    for (svr = 0; svr < stripe_factor; svr++){
        /* compute the "actual" server index */
        uint64_t idx = (start_strip+svr) % stripe_factor;
        /* if object's first offset is after wraparound, need an
         * additional strip_size to compensate */
        obj_offs[idx] = start_obj_off + 
            ((idx < start_strip) ? strip_size : 0);
        if (rem == 0) {
            obj_lens[idx] = 0;
            obj_offs[idx] = 0;
        }
        else if (strip_size >= rem){
            /* we've hit the last strip in the request */
            obj_lens[idx] = rem;
            rem = 0;
        }
        else{
            /* add all strips hitting this server */
            uint64_t nstripes = (rem-strip_size) / stripe_size;
            obj_lens[idx] = strip_size + nstripes*strip_size;
            /* does this server have the last strip? */
            if (end_strip % stripe_factor == idx){
                /* two cases here: 
                 * - last strip covers entire byte extent (%=0) - already added 
                 *   as part of nstripes 
                 * - straggling bytes: (%>0) - add it */
                obj_lens[idx] += rem % strip_size;
            }
            rem -= strip_size;
        }
    }
    /* readjust start_strip for the real offset */
    obj_offs[start_strip] += log_off_adj;
    obj_lens[start_strip] -= log_off_adj;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
