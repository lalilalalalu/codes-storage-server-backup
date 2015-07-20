/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef MSG_H
#define MSG_H

#include "codes/lp-msg.h"
#include <ross.h>

enum request_type {
    /* client <---> triton-server request types */ 
    REQ_OPEN,
    REQ_READ,
    REQ_WRITE,
}; 

typedef struct request_params request_params;
typedef struct triton_cli_callback triton_cli_callback;
typedef struct triton_io_greq triton_io_greq;
typedef struct triton_io_gresp triton_io_gresp;

struct request_params
{
    enum request_type req_type;
    int create; /* create flag for open op */
    uint64_t oid;
    uint64_t xfer_offset;
    uint64_t xfer_size;
};

struct triton_cli_callback {
    /* header to set client message to */
    msg_header header;
    /* ID to pass back to client, used for tracking concurrent events */
    unsigned long op_index; 
    /* size of the callback message structure */
    int event_size;
    /* offset within the client message struct of the message header */
    int header_offset;
    /* offset within the client message struct of the response structure */
    int resp_offset;
};


/* generalized I/O request struct from an arbitrary client to a triton server */
struct triton_io_greq {
    triton_cli_callback callback;
    request_params req;
};

/* generalized I/O request struct from a triton server to an arbitrary client */
struct triton_io_gresp{
    int ack_status;
    int resp_size;
    unsigned long op_index;
    request_params req;
};

/* client-to-server request */
void triton_send_request(triton_io_greq *r, tw_lp *sender, int model_net_id);
void triton_send_request_rev(uint64_t req_size, tw_lp *sender, int model_net_id);

/* server-to-client response */
void triton_send_response(
        triton_cli_callback *c, /* original client request */
        request_params *req,    /* provided by client */
        tw_lp *sender, 
        int model_net_id, 
        int payload_size,
        int ack_status); /* 0 on success, non-zero otherwise */
void triton_send_response_rev(tw_lp *sender, int model_net_id, int payload_size);

/* data structure utilities (setters) */
void msg_set_cli_callback(unsigned long op_index, int event_size, 
        int header_offset, int resp_offset, triton_cli_callback *c);
void msg_set_request(enum request_type req_type, int create, uint64_t oid,
        uint64_t xfer_offset, uint64_t xfer_size, request_params *r);

#endif /* end of include guard: ROSD_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
