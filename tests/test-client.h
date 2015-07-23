/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef TEST_CLIENT_H
#define TEST_CLIENT_H

/* registers the lp type with ross */
void test_client_register(void);
/* configures the lp given the global config object */
void test_client_configure(int model_net_id);

#endif /* end of include guard: TEST_CLIENT_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 *  indent-tabs-mode: nil
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
