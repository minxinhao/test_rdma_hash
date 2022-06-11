#include <arpa/inet.h>
#include <unistd.h>
#include <malloc.h>
#include <libpmem.h>

#include "sock.h"
#include "ib.h"
#include "debug.h"
#include "config.h"
#include "setup_ib.h"
#include "NVM.h"

struct IBRes ib_res;

void set_msg(char* buf,int msg_size,int id){
    buf[msg_size-1]=0;
    for(int i = 0 ; i < msg_size-1 ; i++){
        buf[i]='0'+ id;
    }
}

int connect_qp_server ()
{
    int			 ret		= 0, n = 0, i = 0;
    int			 sockfd		= 0;
    int			peer_sockfd	= 0;
    struct sockaddr_in	 peer_addr;
    socklen_t		 peer_addr_len	= sizeof(struct sockaddr_in);
    char sock_buf[64]			= {'\0'};
    struct QPInfo	local_qp_info, remote_qp_info;
    struct RemoteAddr local_addr, remote_addr	;

    sockfd = sock_create_bind(config_info.ip_address,config_info.sock_port);
    check(sockfd > 0, "Failed to create server socket.");
    listen(sockfd, 5);

    peer_sockfd = accept(sockfd, (struct sockaddr *)&peer_addr,&peer_addr_len);
    check (peer_sockfd > 0, "Failed to create peer_sockfd");


    /* init local qp_info */
	local_qp_info.lid	= ib_res.port_attr.lid; 
	local_qp_info.qp_num = ib_res.qp->qp_num;
    local_addr.remote_addr = (uint64_t)ib_res.mr->addr;
    local_addr.rkey = ib_res.mr->rkey;

    /* get qp_info from client */
	ret = sock_get_qp_info (peer_sockfd, &remote_qp_info);
	check (ret == 0, "Failed to get qp_info from client[%d]", i);
    ret = sock_get_remote_addr (peer_sockfd, &remote_addr);
	check (ret == 0, "Failed to get remore_addr from client[%d]", i);
    
    /* send qp_info to client */
	ret = sock_set_qp_info (peer_sockfd,&local_qp_info);
	check (ret == 0, "Failed to send qp_info to client" );
    ret = sock_set_remote_addr (peer_sockfd,&local_addr);
	check (ret == 0, "Failed to send qp_info to client" );

    log_info("local_addr: addr:%lx rkey:%u",local_addr.remote_addr,local_addr.rkey);
    log_info("remote_addr: addr:%lx rkey:%u",remote_addr.remote_addr,remote_addr.rkey);
    ib_res.rkey = remote_addr.rkey;
    ib_res.remote_addr = remote_addr.remote_addr;
    // ib_res.rkey = local_addr.rkey;
    // ib_res.remote_addr = local_addr.remote_addr;

    /* change send QP state to RTS */
    log (LOG_SUB_HEADER, "Start of IB Config");
	ret = modify_qp_to_rts (ib_res.qp, remote_qp_info.qp_num, remote_qp_info.lid);
	check (ret == 0, "Failed to modify qp to rts");

	log ("\tqp[%"PRIu32"] <-> qp[%"PRIu32"]", ib_res.qp->qp_num, remote_qp_info.qp_num);
    log (LOG_SUB_HEADER, "End of IB Config");

    /* sync with clients */
	n = sock_read (peer_sockfd, sock_buf, sizeof(SOCK_SYNC_MSG));
	check (n == sizeof(SOCK_SYNC_MSG), "Failed to receive sync from client");
    
	n = sock_write (peer_sockfd, sock_buf, sizeof(SOCK_SYNC_MSG));
	check (n == sizeof(SOCK_SYNC_MSG), "Failed to write sync to client");
	
	close (peer_sockfd);
    close (sockfd);

    return 0;

 error:
    if (peer_sockfd > 0) {
        close (peer_sockfd);
    }

    if (sockfd > 0) {
        close (sockfd);
    }
    
    return -1;
}

int connect_qp_client ()
{
    int ret	       = 0, n = 0, i = 0;
    int peer_sockfd   = 0;
    char sock_buf[64]  = {'\0'};

    struct QPInfo	local_qp_info, remote_qp_info;
    struct RemoteAddr local_addr, remote_addr	;

	peer_sockfd = sock_create_connect (config_info.ip_address,config_info.sock_port);
	check (peer_sockfd > 0, "Failed to create peer_sockfd");


	local_qp_info.lid     = ib_res.port_attr.lid; 
	local_qp_info.qp_num  = ib_res.qp->qp_num; 
    local_addr.remote_addr = (uint64_t)ib_res.mr->addr;
    local_addr.rkey = ib_res.mr->rkey;

    /* send qp_info to server */
	ret = sock_set_qp_info (peer_sockfd, &local_qp_info);
	check (ret == 0, "Failed to send qp_info to server");
    ret = sock_set_remote_addr (peer_sockfd,&local_addr);
	check (ret == 0, "Failed to send qp_info to client" );


    /* get qp_info from server */    
	ret = sock_get_qp_info (peer_sockfd, &remote_qp_info);
	check (ret == 0, "Failed to get qp_info from server");
    ret = sock_get_remote_addr (peer_sockfd, &remote_addr);
	check (ret == 0, "Failed to get remore_addr from client[%d]", i);
    
    log_info("local_addr: addr:%lx rkey:%u",local_addr.remote_addr,local_addr.rkey);
    log_info("remote_addr: addr:%lx rkey:%u",remote_addr.remote_addr,remote_addr.rkey);
    ib_res.rkey = remote_addr.rkey;
    ib_res.remote_addr = remote_addr.remote_addr;
    // ib_res.rkey = local_addr.rkey;
    // ib_res.remote_addr = local_addr.remote_addr;

    /* change QP state to RTS */
    /* send qp_info to client */
    int peer_ind = -1;
    int j        = 0;
    log (LOG_SUB_HEADER, "IB Config");

	ret = modify_qp_to_rts (ib_res.qp, remote_qp_info.qp_num, remote_qp_info.lid);
	check (ret == 0, "Failed to modify qp to rts");
    
	log ("\tqp[%"PRIu32"] <-> qp[%"PRIu32"]", ib_res.qp->qp_num, remote_qp_info.qp_num);
    log (LOG_SUB_HEADER, "End of IB Config");

    /* sync with server */
	n = sock_write (peer_sockfd, sock_buf, sizeof(SOCK_SYNC_MSG));
	check (n == sizeof(SOCK_SYNC_MSG), "Failed to write sync to client");
    
	n = sock_read (peer_sockfd, sock_buf, sizeof(SOCK_SYNC_MSG));
	check (n == sizeof(SOCK_SYNC_MSG), "Failed to receive sync from client");

	close (peer_sockfd);

    return 0;

 error:
    if (peer_sockfd > 0) {
		close (peer_sockfd);
    }    
    return -1;
}

int setup_ib ()
{
    int	ret		         = 0;
    int i                        = 0;
    uint64_t len_size;
    int is_mem;
    struct ibv_device **dev_list = NULL;    
    memset (&ib_res, 0, sizeof(struct IBRes));

    /* get IB device list */
    dev_list = ibv_get_device_list(NULL);
    check(dev_list != NULL, "Failed to get ib device list.");

    /* create IB context */
    ib_res.ctx = ibv_open_device(dev_list[0]);
    check(ib_res.ctx != NULL, "Failed to open ib device.");
    
    /* allocate protection domain */
    ib_res.pd = ibv_alloc_pd(ib_res.ctx);
    check(ib_res.pd != NULL, "Failed to allocate protection domain.");

    /* query IB port attribute */
    ret = ibv_query_port(ib_res.ctx, IB_PORT, &ib_res.port_attr);
    check(ret == 0, "Failed to query IB port information.");
    
    /* register mr */
    /* set the buf_size twice as large as msg_size * num_concurr_msgs */
    /* the recv buffer occupies the first half while the sending buffer */
    /* occupies the second half */
    /* assume all msgs are of the same content */
    ib_res.ib_buf_size = config_info.msg_size * config_info.num_concurr_msgs;
    // ib_res.ib_buf      = (char *) memalign (4096, ib_res.ib_buf_size);
    ib_res.ib_buf = get_pmem_buf(ib_res.ib_buf_size);

    check (ib_res.ib_buf != NULL, "Failed to allocate ib_buf");

    ib_res.mr = ibv_reg_mr (ib_res.pd, (void *)ib_res.ib_buf,
			    ib_res.ib_buf_size,
			    IBV_ACCESS_LOCAL_WRITE |
			    IBV_ACCESS_REMOTE_READ |
			    IBV_ACCESS_REMOTE_WRITE);
    check (ib_res.mr != NULL, "Failed to register mr");
    
    /* query IB device attr */
    ret = ibv_query_device(ib_res.ctx, &ib_res.dev_attr);
    check(ret==0, "Failed to query device");
    
    /* create cq */
    // printf("ib_res.dev_attr.max_cqe:%d\n",ib_res.dev_attr.max_cqe);
    ib_res.cq = ibv_create_cq (ib_res.ctx, 512, NULL, NULL, 0);
    // ib_res.cq = ibv_create_cq (ib_res.ctx, ib_res.dev_attr.max_cqe,  NULL, NULL, 0);
    check (ib_res.cq != NULL, "Failed to create cq");

    /* create srq */
    struct ibv_srq_init_attr srq_init_attr = {
	// .attr.max_wr  = ib_res.dev_attr.max_srq_wr,
	.attr.max_wr  = MAX_SRP_WR,
	.attr.max_sge = 1,
    };

    ib_res.srq = ibv_create_srq (ib_res.pd, &srq_init_attr);

    /* create qp */
    struct ibv_qp_init_attr qp_init_attr = {
        .send_cq = ib_res.cq,
        .recv_cq = ib_res.cq,
        .srq     = ib_res.srq,
        .cap = {
            // .max_send_wr = ib_res.dev_attr.max_qp_wr,
            // .max_recv_wr = ib_res.dev_attr.max_qp_wr,
            .max_send_wr = MAX_QP_WR,
            .max_recv_wr = MAX_QP_WR,
            .max_send_sge = 1,
            .max_recv_sge = 1,
        },
        .qp_type = IBV_QPT_RC,
    };

    ib_res.qp = ibv_create_qp (ib_res.pd, &qp_init_attr);
    check (ib_res.qp != NULL, "Failed to create qp");


    /* connect QP */
    if (config_info.is_server) {
        ret = connect_qp_server ();
    } else {
        ret = connect_qp_client ();
    }
    check (ret == 0, "Failed to connect qp");

    ibv_free_device_list (dev_list);
    return 0;

 error:
    if (dev_list != NULL) {
        ibv_free_device_list (dev_list);
    }
    return -1;
}

void close_ib_connection ()
{
    int i;

    if (ib_res.qp != NULL) {
        ibv_destroy_qp (ib_res.qp);
    }

    if (ib_res.srq != NULL) {
        ibv_destroy_srq (ib_res.srq);
    }

    if (ib_res.cq != NULL) {
        ibv_destroy_cq (ib_res.cq);
    }

    if (ib_res.mr != NULL) {
        ibv_dereg_mr (ib_res.mr);
    }

    if (ib_res.pd != NULL) {
        ibv_dealloc_pd (ib_res.pd);
    }

    if (ib_res.ctx != NULL) {
        ibv_close_device (ib_res.ctx);
    }

    if (ib_res.ib_buf != NULL) {
        // free (ib_res.ib_buf);
        free_pmem_buf(ib_res.ib_buf);
    }
}
