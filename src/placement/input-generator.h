/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef INPUT_GENERATOR_H
#define INPUT_GENERATOR_H

#include <stdint.h>

struct server_idx_infile
{
    unsigned int server_index;
    off_t offset;
    unsigned int obj_count;
};

#endif /* INPUT_GENERATOR_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
