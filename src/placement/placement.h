/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef PLACEMENT_H
#define PLACEMENT_H

#include <stdint.h>

/* set placement parameters: placement type and number of servers */
void placement_set(char* type, unsigned int num_servers);

/* distance between two object/server ids */
uint64_t placement_distance(uint64_t a, uint64_t b);

/* convert server id to index (from 0 to num_servers-1) */
unsigned int placement_id_to_index(uint64_t id);

/* convert server index (from 0 to num_servers-1) to an id */
uint64_t placement_index_to_id(unsigned int idx);

/* calculate the N closest servers to a given object */
void placement_find_closest(uint64_t obj, unsigned int replication, 
    unsigned long* server_idxs);

/* generate a set of OIDs for striping a file */
void placement_create_striped(unsigned long file_size, 
  unsigned int replication, unsigned int max_stripe_width, 
  unsigned int strip_size,
  unsigned int* num_objects,
  uint64_t *oids, unsigned long *sizes);

#endif /* PLACEMENT_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
