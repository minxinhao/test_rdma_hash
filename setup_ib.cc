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

int connect(){
    if(config_info.is_server){
        struct sockaddr_in	 peer_addr;
        socklen_t		 peer_addr_len	= sizeof(struct sockaddr_in);
        char sock_buf[64]			= {'\0'};
        struct QPInfo	local_qp_info, remote_qp_info;
        struct RemoteAddr local_addr, remote_addr	;

        config_info.sockfd = sock_create_bind(config_info.ip_address,config_info.sock_port);
        check(config_info.sockfd > 0, "Failed to create server socket.");
        listen(config_info.sockfd, 5);
        
        config_info.peer_sockfd = accept(config_info.sockfd, (struct sockaddr *)&peer_addr,&peer_addr_len);
        check (config_info.peer_sockfd > 0, "Failed to create peer_sockfd");
    }else{
        config_info.peer_sockfd = sock_create_connect (config_info.ip_address,config_info.sock_port);
        check (config_info.peer_sockfd > 0, "Failed to create peer_sockfd");
    }
    
    return 0;
error:
    if(config_info.peer_sockfd > 0) close(config_info.peer_sockfd);
    if(config_info.is_server && config_info.sockfd > 0) close (config_info.sockfd);
    return -1;
}

void disconnect(){
    close(config_info.peer_sockfd);
    if(config_info.is_server){
        close(config_info.sockfd);
    }
}

int sync_server_client(){
    /* sync with server */
    int n = 0;
    char sock_buf[64]			= {'\0'};
	n = sock_write (config_info.peer_sockfd, sock_buf, sizeof(SOCK_SYNC_MSG));
	check (n == sizeof(SOCK_SYNC_MSG), "Failed to write sync to client");
    
	n = sock_read (config_info.peer_sockfd, sock_buf, sizeof(SOCK_SYNC_MSG));
	check (n == sizeof(SOCK_SYNC_MSG), "Failed to receive sync from client");
    
    return 0;
error:
    return -1;
}

int connect_qp_server ()
{
    int			 ret		= 0, n = 0, i = 0;
    struct sockaddr_in	 peer_addr;
    socklen_t		 peer_addr_len	= sizeof(struct sockaddr_in);
    char sock_buf[64]			= {'\0'};
    struct QPInfo	local_qp_info, remote_qp_info;
    struct RemoteAddr local_addr, remote_addr	;

    /* init local qp_info */
	local_qp_info.lid	= ib_res.port_attr.lid; 
	local_qp_info.qp_num = ib_res.qp->qp_num;
    memcpy(&local_qp_info.gid,(char*)&ib_res.gid,16);
    local_addr.remote_addr = (uint64_t)ib_res.mr->addr;
    local_addr.rkey = ib_res.mr->rkey;

    /* get qp_info from client */
	ret = sock_get(config_info.peer_sockfd, (char*)&remote_qp_info,sizeof(struct QPInfo));
	check (ret == 0, "Failed to get qp_info from client[%d]", i);
    ret = sock_get(config_info.peer_sockfd, (char*)&remote_addr,sizeof(struct RemoteAddr));
	check (ret == 0, "Failed to get remore_addr from client[%d]", i);
    
    /* send qp_info to client */
	ret = sock_set(config_info.peer_sockfd,(char*)&local_qp_info,sizeof(struct QPInfo));
	check (ret == 0, "Failed to send qp_info to client" );
    ret = sock_set(config_info.peer_sockfd,(char*)&local_addr,sizeof(struct RemoteAddr));
	check (ret == 0, "Failed to send qp_info to client" );

    log_info("local_addr: addr:%lx rkey:%u",local_addr.remote_addr,local_addr.rkey);
    log_info("remote_addr: addr:%lx rkey:%u",remote_addr.remote_addr,remote_addr.rkey);
    ib_res.rkey = remote_addr.rkey;
    ib_res.remote_addr = remote_addr.remote_addr;

    /* change send QP state to RTS */
	ret = modify_qp_to_rts (ib_res.qp, &remote_qp_info,config_info.roce_flag);
	check (ret == 0, "Failed to modify qp to rts");
    
    ret = sync_server_client();
	check (ret == 0, "Failed to Sync with client to exit");
    return 0;

 error:
    return -1;
}

int connect_qp_client ()
{
    int ret	       = 0, n = 0, i = 0;
    char sock_buf[64]  = {'\0'};
    struct QPInfo	local_qp_info,remote_qp_info;
    struct RemoteAddr local_addr, remote_addr	;
    int peer_ind = -1;
    int j        = 0;

	local_qp_info.lid     = ib_res.port_attr.lid; 
	local_qp_info.qp_num  = ib_res.qp->qp_num; 
    memcpy(&local_qp_info.gid,(char*)&ib_res.gid,16);
    local_addr.remote_addr = (uint64_t)ib_res.mr->addr;
    local_addr.rkey = ib_res.mr->rkey;

    /* send qp_info to server */
	ret = sock_set(config_info.peer_sockfd, (char*)&local_qp_info,sizeof(struct QPInfo));
	check (ret == 0, "Failed to send qp_info to server");
    ret = sock_set(config_info.peer_sockfd,(char*)&local_addr,sizeof(struct RemoteAddr));
	check (ret == 0, "Failed to send qp_info to client" );

    /* get qp_info from server */    
	ret = sock_get(config_info.peer_sockfd, (char*)&remote_qp_info,sizeof(struct QPInfo));
	check (ret == 0, "Failed to get qp_info from server");
    ret = sock_get(config_info.peer_sockfd, (char*)&remote_addr,sizeof(struct RemoteAddr));
	check (ret == 0, "Failed to get remore_addr from client[%d]", i);
    
    log_info("local_addr: addr:%lx rkey:%u",local_addr.remote_addr,local_addr.rkey);
    log_info("remote_addr: addr:%lx rkey:%u",remote_addr.remote_addr,remote_addr.rkey);
    ib_res.rkey = remote_addr.rkey;
    ib_res.remote_addr = remote_addr.remote_addr;

    /* change QP state to RTS */
    /* send qp_info to client */
	ret = modify_qp_to_rts (ib_res.qp, &remote_qp_info,config_info.roce_flag);
	check (ret == 0, "Failed to modify qp to rts");

    ret = sync_server_client();
	check (ret == 0, "Failed to Sync with server to exit");

    return 0;
 error:
    if (config_info.peer_sockfd > 0) close(config_info.peer_sockfd);
    return -1;
}

int setup_ib ()
{
    int	ret		         = 0;
    int i                        = 0;
    uint64_t len_size;
    struct ibv_device **dev_list = NULL;  
    struct ibv_srq_init_attr srq_init_attr;
    struct ibv_qp_init_attr qp_init_attr;  
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
    
    /* query gid */
    if(config_info.roce_flag){
        ret = ibv_query_gid(ib_res.ctx, IB_PORT, 0, &ib_res.gid);
        check(ret == 0, "Failed to query GID information for ROCE Network.");
    }

    /* register mr */
    /* set the buf_size twice as large as msg_size * num_concurr_msgs */
    /* the recv buffer occupies the first half while the sending buffer */
    /* occupies the second half */
    /* assume all msgs are of the same content */
    ib_res.ib_buf_size = PM_SIZE;
    ib_res.ib_buf = get_pmem_buf(ib_res.ib_buf_size);
    check (ib_res.ib_buf != NULL, "Failed to allocate ib_buf");
    log_message("ib_res.ib_buf_size : %ld GB",(ib_res.ib_buf_size)/(1<<30))
    ib_res.mr = ibv_reg_mr (ib_res.pd, (void *)ib_res.ib_buf,ib_res.ib_buf_size,IBV_ACCESS_LOCAL_WRITE |IBV_ACCESS_REMOTE_READ |IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
    check (ib_res.mr != NULL, "Failed to register mr");
    
    /* query IB device attr */
    ret = ibv_query_device(ib_res.ctx, &ib_res.dev_attr);
    check(ret==0, "Failed to query device");
    
    /* create cq */
    ib_res.cq = ibv_create_cq (ib_res.ctx, 512, NULL, NULL, 0);
    check (ib_res.cq != NULL, "Failed to create cq");

    /* create srq */
	// .attr.max_wr  = ib_res.dev_attr.max_srq_wr,
	srq_init_attr.attr.max_wr  = MAX_SRP_WR;
	srq_init_attr.attr.max_sge = 1;
    ib_res.srq = ibv_create_srq (ib_res.pd, &srq_init_attr);

    /* create qp */
    memset(&qp_init_attr, 0, sizeof(struct ibv_qp_init_attr));
    qp_init_attr.send_cq = ib_res.cq;
    qp_init_attr.recv_cq = ib_res.cq;
    qp_init_attr.srq     = ib_res.srq;
    qp_init_attr.cap = {
        // .max_send_wr = ib_res.dev_attr.max_qp_wr,
        // .max_recv_wr = ib_res.dev_attr.max_qp_wr,
        .max_send_wr = MAX_QP_WR,
        .max_recv_wr = MAX_QP_WR,
        .max_send_sge = 1,
        .max_recv_sge = 1,
    };
    qp_init_attr.sq_sig_all = 0;
    qp_init_attr.qp_type = IBV_QPT_RC;
    ib_res.qp = ibv_create_qp (ib_res.pd, &qp_init_attr);
    check (ib_res.qp != NULL, "Failed to create qp");


    /* connect QP */
    connect();
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
    disconnect();
}

void wait_poll(uint64_t wr_id){
    struct ibv_wc wc; 
    int n=0;
    while(true){
        n = ibv_poll_cq(ib_res.cq, 1, &wc); 
        if(n && (wc.wr_id == wr_id)){
            if(unlikely(wc.status!=IBV_WC_SUCCESS)) log_message("wait_poll with error:%d",wc.status);
            break;
        }
    }
}