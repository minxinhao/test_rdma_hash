#include <arpa/inet.h>
#include <unistd.h>

#include "ib.h"
#include "debug.h"
#define MAX_POST_LIST 2048

uint64_t wr_id_cnt;
uint64_t wr_offset;
struct ibv_sge write_sge_arr[MAX_POST_LIST+1];
struct ibv_send_wr send_wr_arr[MAX_POST_LIST+1];

uint64_t get_wr_id(){
    return ++wr_id_cnt;
}

int modify_qp_to_rts (struct ibv_qp *qp, uint32_t target_qp_num, uint16_t target_lid)
{
    int ret = 0;

    /* change QP state to INIT */
    {
	struct ibv_qp_attr qp_attr = {
	    .qp_state        = IBV_QPS_INIT,
	    .pkey_index      = 0,
	    .port_num        = IB_PORT,
	    .qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
	                       IBV_ACCESS_REMOTE_READ |
	                       IBV_ACCESS_REMOTE_ATOMIC |
	                       IBV_ACCESS_REMOTE_WRITE,
	};

	ret = ibv_modify_qp (qp, &qp_attr,
			 IBV_QP_STATE | IBV_QP_PKEY_INDEX |
			 IBV_QP_PORT  | IBV_QP_ACCESS_FLAGS);
	check (ret == 0, "Failed to modify qp to INIT.");
    }

    /* Change QP state to RTR */
    {
	struct ibv_qp_attr  qp_attr = {
	    .qp_state           = IBV_QPS_RTR,
	    .path_mtu           = IB_MTU,
	    .dest_qp_num        = target_qp_num,
	    .rq_psn             = 0,
	    .max_dest_rd_atomic = 1,
	    .min_rnr_timer      = 12,
	    .ah_attr.is_global  = 0,
	    .ah_attr.dlid       = target_lid,
	    .ah_attr.sl         = IB_SL,
	    .ah_attr.src_path_bits = 0,
	    .ah_attr.port_num      = IB_PORT,
	};
	log_info("dest_qp_num:%u dlid:%u\n",target_qp_num,target_lid);
	ret = ibv_modify_qp(qp, &qp_attr,
			    IBV_QP_STATE | IBV_QP_AV |
			    IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
			    IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
			    IBV_QP_MIN_RNR_TIMER);
	check (ret == 0, "Failed to change qp to rtr.");
    }

    /* Change QP state to RTS */
    {
	struct ibv_qp_attr  qp_attr = {
	    .qp_state      = IBV_QPS_RTS,
	    .timeout       = 14,
	    .retry_cnt     = 7,
	    .rnr_retry     = 7,
	    .sq_psn        = 0,
	    .max_rd_atomic = 1,
	};

	ret = ibv_modify_qp (qp, &qp_attr,
			     IBV_QP_STATE | IBV_QP_TIMEOUT |
			     IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
			     IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
	check (ret == 0, "Failed to modify qp to RTS.");
    }

    return 0;
 error:
    return -1;
}

int post_send_woimm (uint32_t req_size, uint32_t lkey, uint64_t wr_id, struct ibv_qp *qp, char *buf)
{
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list = {
	.addr   = (uintptr_t) buf,
	.length = req_size,
	.lkey   = lkey
    };

    struct ibv_send_wr send_wr = {
	.wr_id      = wr_id,
	.sg_list    = &list,
	.num_sge    = 1,
	.opcode     = IBV_WR_SEND,
	.send_flags = IBV_SEND_SIGNALED,
    };

    ret = ibv_post_send (qp, &send_wr, &bad_send_wr);
    return ret;
}

int post_send (uint32_t req_size, uint32_t lkey, uint64_t wr_id,
	       uint32_t imm_data, struct ibv_qp *qp, char *buf)
{
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list = {
	.addr   = (uintptr_t) buf,
	.length = req_size,
	.lkey   = lkey
    };

    struct ibv_send_wr send_wr = {
	.wr_id      = wr_id,
	.sg_list    = &list,
	.num_sge    = 1,
	.opcode     = IBV_WR_SEND_WITH_IMM,
	.send_flags = IBV_SEND_SIGNALED,
	.imm_data   = htonl (imm_data)
    };

    ret = ibv_post_send (qp, &send_wr, &bad_send_wr);
    return ret;
}

int post_write (uint32_t req_size, uint32_t lkey, uint64_t wr_id, 
	       uint32_t imm_data, uint32_t rkey, uint64_t remote_addr ,struct ibv_qp *qp, char *buf){
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list = {
	.addr   = (uintptr_t) buf,
	.length = req_size,
	.lkey   = lkey
    };

    struct ibv_send_wr send_wr = {
	.wr_id      = wr_id,
	.sg_list    = &list,
	.num_sge    = 1,
	.opcode     = IBV_WR_RDMA_WRITE,
	.send_flags = IBV_SEND_SIGNALED,
    };
	send_wr.wr.rdma.rkey = rkey;
	send_wr.wr.rdma.remote_addr = remote_addr;

    ret = ibv_post_send (qp, &send_wr, &bad_send_wr);
    return ret;
}

int post_read(uint32_t req_size, uint32_t lkey, uint64_t wr_id, 
	       uint32_t imm_data, uint32_t rkey, uint64_t remote_addr ,struct ibv_qp *qp, char *buf){
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list = {
	.addr   = (uintptr_t) buf,
	.length = req_size,
	.lkey   = lkey
    };

    struct ibv_send_wr send_wr = {
	.wr_id      = wr_id,
	.sg_list    = &list,
	.num_sge    = 1,
	.opcode     = IBV_WR_RDMA_READ,
	.send_flags = IBV_SEND_SIGNALED,
	.imm_data   = htonl (imm_data),
    };
	send_wr.wr.rdma.rkey = rkey;
	send_wr.wr.rdma.remote_addr = remote_addr;

    ret = ibv_post_send (qp, &send_wr, &bad_send_wr);
    return ret;
}

//post read after write for the same address to flush the data
int post_raw(uint32_t req_size, uint32_t lkey, uint64_t wr_id, 
	       uint32_t imm_data, uint32_t rkey, uint64_t remote_addr ,struct ibv_qp *qp, char *buf){
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;
    
    struct ibv_sge write_sge = {
	.addr   = (uintptr_t) buf,
	.length = req_size,
	.lkey   = lkey
    };

    struct ibv_send_wr write_wr = {
	.wr_id      = wr_id,
	.sg_list    = &write_sge,
	.num_sge    = 1,
	.opcode     = IBV_WR_RDMA_WRITE,
	// .send_flags = IBV_SEND_SIGNALED,
    };
	write_wr.wr.rdma.rkey = rkey;
	write_wr.wr.rdma.remote_addr = remote_addr;

    struct ibv_sge read_sge = {
	.addr   = (uintptr_t) (buf+req_size),
	.length = req_size,
	.lkey   = lkey
    };

    struct ibv_send_wr read_wr = {
	.wr_id      = wr_id,
	.sg_list    = &read_sge,
	.num_sge    = 1,
	.opcode     = IBV_WR_RDMA_READ,
	.send_flags = IBV_SEND_SIGNALED,
    };
	read_wr.wr.rdma.rkey = rkey;
	read_wr.wr.rdma.remote_addr = remote_addr;

	write_wr.next = &read_wr;

    ret = ibv_post_send (qp, &write_wr, &bad_send_wr);
    return ret;
}

int post_srq_recv (uint32_t req_size, uint32_t lkey, uint64_t wr_id, 
		   struct ibv_srq *srq, char *buf)
{
    int ret = 0;
    struct ibv_recv_wr *bad_recv_wr;

    struct ibv_sge list = {
	.addr   = (uintptr_t) buf,
	.length = req_size,
	.lkey   = lkey
    };

    struct ibv_recv_wr recv_wr = {
	.wr_id   = wr_id,
	.sg_list = &list,
	.num_sge = 1
    };

    ret = ibv_post_srq_recv (srq, &recv_wr, &bad_recv_wr);
    return ret;
}


// post n wr per-batch in s post-list 
// batch_size shoule below MAX_POST_LIST
int post_write_batch(uint32_t batch_size,uint32_t req_size, uint32_t lkey, uint64_t wr_id, 
	       uint32_t imm_data, uint32_t rkey, uint64_t remote_addr ,struct ibv_qp *qp, char *buf){
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

	uint64_t old_offset = wr_offset;
	for(uint32_t i = 0 ; i < batch_size ; i++){
		write_sge_arr[wr_offset].addr = (uintptr_t) buf+i*req_size;
		write_sge_arr[wr_offset].length = req_size;
		write_sge_arr[wr_offset].lkey = lkey;

		send_wr_arr[wr_offset].wr_id = wr_id;
		send_wr_arr[wr_offset].sg_list = &write_sge_arr[wr_offset];
		send_wr_arr[wr_offset].num_sge = 1;
		send_wr_arr[wr_offset].opcode = IBV_WR_RDMA_WRITE;
		send_wr_arr[wr_offset].wr.rdma.rkey = rkey;
		send_wr_arr[wr_offset].wr.rdma.remote_addr = remote_addr+i*req_size;
		send_wr_arr[wr_offset].next = &send_wr_arr[wr_offset+1];
		wr_offset = (wr_offset+1)%MAX_POST_LIST;
	}
	// the last one should be signaled
	send_wr_arr[old_offset+batch_size-1].send_flags = IBV_SEND_SIGNALED;
	send_wr_arr[old_offset+batch_size-1].next = NULL;

    ret = ibv_post_send (qp, &send_wr_arr[old_offset], &bad_send_wr);
    return ret;
}