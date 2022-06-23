#ifndef IB_H_
#define IB_H_

#include <inttypes.h>
#include <sys/types.h>
#include <endian.h>
#include <byteswap.h>
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <stdbool.h>

#include "RACE.h"

#define IB_MTU			IBV_MTU_4096
#define IB_PORT			1
#define IB_SL			0
#define IB_WR_ID_STOP		0xE000000000000000
#define SIG_INTERVAL            1000
#define NUM_WARMING_UP_OPS      1000
#define TOT_NUM_OPS             10000000

extern uint64_t wr_id_cnt;
uint64_t get_wr_id();


struct QPInfo {
    uint16_t lid;
    uint32_t qp_num;
    uint8_t gid[16];
}__attribute__ ((packed));

struct RemoteAddr {
    uint64_t	remote_addr;
    uint32_t	rkey;
}__attribute__ ((packed));


enum MsgType {
    MSG_CTL_START = 100,
    MSG_CTL_STOP,
};

int modify_qp_to_rts (struct ibv_qp *qp, struct QPInfo* remote_qpinfo, bool roce_flag);

int post_send (uint32_t req_size, uint32_t lkey, uint64_t wr_id, 
	       uint32_t imm_data, struct ibv_qp *qp, char *buf);

int post_send_woimm (uint32_t req_size, uint32_t lkey, uint64_t wr_id, struct ibv_qp *qp, char *buf);

int post_write (uint32_t req_size, uint32_t lkey, uint64_t wr_id, uint32_t rkey, 
                uint64_t remote_addr ,struct ibv_qp *qp, char *buf);

int post_read (uint32_t req_size, uint32_t lkey, uint64_t wr_id, 
	           uint32_t rkey, uint64_t remote_addr ,struct ibv_qp *qp, char *buf);

int post_read2(uint32_t req_size, uint32_t lkey, uint64_t wr_id, uint32_t rkey, 
            uint64_t remote_addr_1,uint64_t remote_addr_2 ,struct ibv_qp *qp, char *buf);

int post_raw(uint32_t req_size, uint32_t lkey, uint64_t wr_id, 
	       uint32_t imm_data, uint32_t rkey, uint64_t remote_addr ,struct ibv_qp *qp, char *buf);

int post_write_batch(uint32_t batch_size,uint32_t req_size, uint32_t lkey, uint64_t wr_id, 
	       uint32_t imm_data, uint32_t rkey, uint64_t remote_addr ,struct ibv_qp *qp, char *buf);

int post_srq_recv (uint32_t req_size, uint32_t lkey, uint64_t wr_id, 
		   struct ibv_srq *srq, char *buf);

int post_cas (uint32_t lkey, uint64_t wr_id, uint32_t rkey, uint64_t remote_addr ,
			  struct ibv_qp *qp, char *buf,uint64_t val,uint64_t old);

int post_add (uint32_t lkey, uint64_t wr_id, uint32_t rkey, uint64_t remote_addr ,
			  struct ibv_qp *qp, char *buf,uint64_t val);

int post_write_dir(uint32_t lkey, uint64_t wr_id, uint32_t rkey, uint64_t remote_addr ,
    struct ibv_qp *qp, struct CCEH *cceh_cache,uint64_t local_depth,uint64_t first_seg_loc,uint64_t stride);

#endif /*ib.h*/
