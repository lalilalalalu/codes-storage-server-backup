/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef PLACEMENT_VIRT_H
#define PLACEMENT_VIRT_H

#include "placement-mod.h"

struct placement_mod* placement_mod_virt(void);
void placement_virt_set_factor(int factor, unsigned int num_servers);

#endif /* PLACEMENT_VIRT_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
