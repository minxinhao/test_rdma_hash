#include <arpa/inet.h>
#include <unistd.h>
#include <stdlib.h>
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

int modify_qp_to_rts (struct ibv_qp *qp, struct QPInfo* remote_qpinfo, bool roce_flag)
{
    int ret = 0;
    /* change QP state to INIT */
    {
		struct ibv_qp_attr qp_attr; 
		int flags;
		memset(&qp_attr, 0, sizeof(ibv_qp_attr));
	    qp_attr.qp_state = IBV_QPS_INIT;
	    qp_attr.pkey_index = 0;
	    qp_attr.port_num = IB_PORT;
	    qp_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE |IBV_ACCESS_REMOTE_READ |IBV_ACCESS_REMOTE_ATOMIC |IBV_ACCESS_REMOTE_WRITE;
		flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
		ret = ibv_modify_qp (qp, &qp_attr,flags);
		check (ret == 0, "Failed to modify qp to INIT.");
    }

    /* Change QP state to RTR */
    {
		struct ibv_qp_attr  qp_attr;
		int flags;
		memset(&qp_attr, 0, sizeof(ibv_qp_attr));
	    qp_attr.qp_state = IBV_QPS_RTR;
	    qp_attr.path_mtu = IB_MTU;
	    qp_attr.dest_qp_num = remote_qpinfo->qp_num;
	    qp_attr.rq_psn   = 0;
	    qp_attr.max_dest_rd_atomic = 16;
	    qp_attr.min_rnr_timer      = 12;
	    qp_attr.ah_attr.is_global  = 0;
	    qp_attr.ah_attr.dlid       = remote_qpinfo->lid;
	    qp_attr.ah_attr.sl = IB_SL;
	    qp_attr.ah_attr.src_path_bits = 0;
	    qp_attr.ah_attr.port_num      = IB_PORT;
		if(roce_flag){
			qp_attr.ah_attr.is_global = 1;
			qp_attr.ah_attr.dlid = 0;
			memcpy(&qp_attr.ah_attr.grh.dgid,remote_qpinfo->gid,16);
			qp_attr.ah_attr.grh.sgid_index = 1;
			qp_attr.ah_attr.grh.hop_limit = 1;
			qp_attr.ah_attr.grh.flow_label = 0;
			qp_attr.ah_attr.grh.traffic_class = 0;
			// log_message("ROCE:interface_id %llu subnet_prefix:%llu",remote_qpinfo->interface_id,remote_qpinfo->subnet_prefix);
			log_message("ROCE:interface_id %llu subnet_prefix:%llu",qp_attr.ah_attr.grh.dgid.global.interface_id,qp_attr.ah_attr.grh.dgid.global.subnet_prefix);
		}
		flags = IBV_QP_STATE | IBV_QP_AV |
                IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                IBV_QP_MIN_RNR_TIMER;
		log_info("dest_qp_num:%u dlid:%u\n",remote_qpinfo->qp_num,remote_qpinfo->lid);
		ret = ibv_modify_qp(qp, &qp_attr,flags);
		check (ret == 0, "Failed to change qp to rtr.");
    }

    /* Change QP state to RTS */
    {
		struct ibv_qp_attr  qp_attr; 
		int flags;
		memset(&qp_attr, 0, sizeof(ibv_qp_attr));
	    qp_attr.qp_state      = IBV_QPS_RTS;
	    qp_attr.timeout       = 31;
	    qp_attr.retry_cnt     = 7;
	    qp_attr.rnr_retry     = 0;
	    qp_attr.sq_psn        = 0;
	    qp_attr.max_rd_atomic = 16;

		flags = IBV_QP_STATE | IBV_QP_TIMEOUT |
                IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
		ret = ibv_modify_qp (qp, &qp_attr,flags);
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
	.imm_data   = imm_data
    };

    ret = ibv_post_send (qp, &send_wr, &bad_send_wr);
    return ret;
}

int post_cas (uint32_t lkey, uint64_t wr_id, uint32_t rkey, uint64_t remote_addr ,
			  struct ibv_qp *qp, char *buf,uint64_t val,uint64_t old){
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list ;
	list.addr   = (uintptr_t) buf;
	list.length = sizeof(uint64_t);
	list.lkey   = lkey;

    struct ibv_send_wr send_wr ;
	memset(&send_wr,0,sizeof(struct ibv_send_wr));
	send_wr.wr_id      = wr_id;
	send_wr.sg_list    = &list;
	send_wr.num_sge    = 1;
	send_wr.opcode     = IBV_WR_ATOMIC_CMP_AND_SWP;
	send_wr.send_flags = IBV_SEND_SIGNALED;
	send_wr.wr.atomic.rkey = rkey;
	send_wr.wr.atomic.remote_addr = remote_addr;
	send_wr.wr.atomic.compare_add = old;
	send_wr.wr.atomic.swap = val;

    ret = ibv_post_send (qp, &send_wr, &bad_send_wr);
    return ret;
}

int post_add (uint32_t lkey, uint64_t wr_id, uint32_t rkey, uint64_t remote_addr ,
			  struct ibv_qp *qp, char *buf,uint64_t val){
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list ;
	list.addr   = (uintptr_t) buf;
	list.length = sizeof(uint64_t);
	list.lkey   = lkey;

    struct ibv_send_wr send_wr ;
	memset(&send_wr,0,sizeof(struct ibv_send_wr));
	send_wr.wr_id      = wr_id;
	send_wr.sg_list    = &list;
	send_wr.num_sge    = 1;
	send_wr.opcode     = IBV_WR_ATOMIC_FETCH_AND_ADD;
	send_wr.send_flags = IBV_SEND_SIGNALED;
	send_wr.wr.atomic.rkey = rkey;
	send_wr.wr.atomic.remote_addr = remote_addr;
	send_wr.wr.atomic.compare_add = val;

    ret = ibv_post_send (qp, &send_wr, &bad_send_wr);
    return ret;
}

int post_write (uint32_t req_size, uint32_t lkey, uint64_t wr_id, uint32_t rkey, 
				uint64_t remote_addr ,struct ibv_qp *qp, char *buf){
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
	    	  uint32_t rkey, uint64_t remote_addr ,struct ibv_qp *qp, char *buf){
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

int post_read2(uint32_t req_size, uint32_t lkey, uint64_t wr_id, uint32_t rkey, 
			uint64_t remote_addr_1,uint64_t remote_addr_2 ,struct ibv_qp *qp, char *buf){
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

	struct ibv_sge read_sge[2];
	struct ibv_send_wr read_wr[2];
	memset(read_wr,0,2*sizeof(struct ibv_send_wr));
	read_sge[0].addr = (uintptr_t)buf;
	read_sge[0].length = req_size;
	read_sge[0].lkey = lkey;
	read_wr[0].wr_id = wr_id;
	read_wr[0].sg_list = &read_sge[0];
	read_wr[0].num_sge = 1;
	read_wr[0].opcode = IBV_WR_RDMA_READ;
	read_wr[0].wr.rdma.rkey = rkey;
	read_wr[0].wr.rdma.remote_addr = remote_addr_1;
	read_wr[0].next = &read_wr[1];
	read_wr[0].send_flags = IBV_SEND_SIGNALED;


	read_sge[1].addr = ((uintptr_t)buf)+req_size;
	read_sge[1].length = req_size;
	read_sge[1].lkey = lkey;
	read_wr[1].wr_id = wr_id;
	read_wr[1].sg_list = &read_sge[1];
	read_wr[1].num_sge = 1;
	read_wr[1].opcode = IBV_WR_RDMA_READ;
	read_wr[1].wr.rdma.rkey = rkey;
	read_wr[1].wr.rdma.remote_addr = remote_addr_2;
	read_wr[1].next = NULL;
	read_wr[1].send_flags = IBV_SEND_SIGNALED;

    ret = ibv_post_send (qp, &read_wr[0], &bad_send_wr);
    return ret;
}


int post_write_dir(uint32_t lkey, uint64_t wr_id, uint32_t rkey,uint64_t remote_addr,struct ibv_qp *qp, 
		struct CCEH *cceh_cache,uint64_t local_depth,uint64_t first_seg_loc,uint64_t stride){
	int ret = 0;
    struct ibv_send_wr *bad_send_wr;

	uint64_t old_offset = wr_offset;

	uint64_t cur_seg_loc;
	for(uint64_t i = 0 ; i < stride ; i++){
		cur_seg_loc = (i<<(local_depth+1))|first_seg_loc;
		write_sge_arr[old_offset].addr = (uint64_t)(cceh_cache->dir+cur_seg_loc) ;
		write_sge_arr[old_offset].length = sizeof(struct DirEntry);
		write_sge_arr[old_offset].lkey = lkey;

		send_wr_arr[old_offset].wr_id = wr_id;
		send_wr_arr[old_offset].sg_list = &write_sge_arr[old_offset];
		send_wr_arr[wr_offset].num_sge = 1;
		send_wr_arr[wr_offset].opcode = IBV_WR_RDMA_WRITE;
		send_wr_arr[wr_offset].wr.rdma.rkey = rkey;
		send_wr_arr[wr_offset].wr.rdma.remote_addr = remote_addr+2*sizeof(uint64_t)+cur_seg_loc*sizeof(struct DirEntry);
		send_wr_arr[wr_offset].next = &send_wr_arr[wr_offset+1];
		wr_offset = (wr_offset+1)%MAX_POST_LIST;
	}

	// the last one should be signaled
	send_wr_arr[old_offset+stride-1].send_flags = IBV_SEND_SIGNALED;
	send_wr_arr[old_offset+stride-1].next = NULL;

    ret = ibv_post_send (qp, &send_wr_arr[old_offset], &bad_send_wr);
    return ret;
}