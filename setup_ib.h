#ifndef SETUP_IB_H_
#define SETUP_IB_H_

#include <infiniband/verbs.h>
#include <libpmem.h>

#define MAX_QP_WR 2048
#define MAX_SRP_WR 2048

struct IBRes {
    struct ibv_context		*ctx;
    struct ibv_pd		*pd;
    struct ibv_mr		*mr;
    struct ibv_cq		*cq;
    struct ibv_qp		*qp;
    struct ibv_srq              *srq;
    struct ibv_port_attr	 port_attr;
    struct ibv_device_attr	 dev_attr;
    uint32_t	rkey;
    uint64_t	remote_addr;

    char   *ib_buf;
    size_t  ib_buf_size;
};

extern struct IBRes ib_res;

void set_msg(char* buf,int msg_size,int id);

int  setup_ib ();
void close_ib_connection ();

int  connect_qp_server ();
int  connect_qp_client ();

#endif /*setup_ib.h*/
