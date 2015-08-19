/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef DIST_H
#define DIST_H

#include <stdint.h>

/* given a simple stripe distribution, a logical request (off,len), and the
 * calculate the offset/lengths for all objects in the stripe set. If the 
 * request does not coincide with an object, its length is set to 0 */
void map_logical_to_physical_objs(
        unsigned int stripe_factor, 
        unsigned int strip_size, 
        uint64_t log_off, 
        uint64_t log_len, 
        uint64_t *obj_offs,
        uint64_t *obj_lens);

#endif /* end of include guard: DIST_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
