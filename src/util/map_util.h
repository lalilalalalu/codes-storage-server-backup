/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef MAP_UTIL_H
#define MAP_UTIL_H

/* utilities and constants for mapping LP ids indexes and vice-versa */

/* rosd LPs */
#define ROSD_GRP_NM "TRITON_GRP"
#define ROSD_LP_NM "rosd"
tw_lpid get_rosd_lpid(int svr_id);
int get_rosd_index(tw_lpid gid);

/* client LPs */
#define CLIENT_GRP_NM "CLIENT_GRP"
#define CLIENT_LP_NM  "triton_client"
tw_lpid get_client_lpid(int client_idx);
int get_client_index(tw_lpid gid);

/* there is only a single barrier lp */
tw_lpid get_barrier_lpid();


#endif /* end of include guard: MAP_UTIL_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
