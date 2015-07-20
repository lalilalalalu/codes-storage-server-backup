/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef PLACEMENT_MULTIRING_H
#define PLACEMENT_MULTIRING_H

#include "placement-mod.h"

struct placement_mod* placement_mod_multiring(void);
void placement_multiring_set_rings(int rings, unsigned int num_servers);

#endif /* PLACEMENT_MULTIRING_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
