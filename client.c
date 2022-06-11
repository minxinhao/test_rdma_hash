#define _GNU_SOURCE
#include <stdlib.h>
#include <stdbool.h>
#include <sys/time.h>

#include "debug.h"
#include "config.h"
#include "setup_ib.h"
#include "ib.h"
#include "client.h"


void *client_thread_func (void *arg)
{
    int         ret		 = 0, n = 0, i = 0, j = 0;
    long	thread_id	 = (long) arg;
    int         msg_size	 = config_info.msg_size;
    int         num_concurr_msgs = config_info.num_concurr_msgs;

    pthread_t   self;
    cpu_set_t   cpuset;

    int                  num_wc		= 20;
    struct ibv_qp	*qp		= ib_res.qp;
    struct ibv_cq       *cq		= ib_res.cq;
    struct ibv_srq      *srq            = ib_res.srq;
    struct ibv_wc       *wc		= NULL;
    uint32_t             lkey           = ib_res.mr->lkey;

    char		*buf_ptr	= ib_res.ib_buf;
    char		*buf_base	= ib_res.ib_buf;
    int			 buf_offset	= 0;
    size_t               buf_size	= ib_res.ib_buf_size;
    uint64_t wr_id ;

    uint32_t		imm_data	= 0;
    int			num_acked_peers = 0;
    bool		start_sending	= false;
    bool		stop		= false;
    struct timeval      start, end;
    long                ops_count	= 0;
    double              duration	= 0.0;
    double              throughput	= 0.0;

    /* set thread affinity */
    // CPU_ZERO (&cpuset);
    // CPU_SET  ((int)thread_id, &cpuset);
    // self = pthread_self ();
    // ret  = pthread_setaffinity_np (self, sizeof(cpu_set_t), &cpuset);
    // check (ret == 0, "thread[%ld]: failed to set thread affinity", thread_id);

    /* pre-post recvs */    
    wc = (struct ibv_wc *) calloc (num_wc, sizeof(struct ibv_wc));
    check (wc != NULL, "thread[%ld]: failed to allocate wc.", thread_id);
	ret = post_srq_recv (msg_size, lkey, (uint64_t)buf_ptr, srq, buf_ptr);
	check(ret==0,"client recv for start fail");

    /* wait for start signal */
    while (true) {
        do {
            n = ibv_poll_cq (cq, num_wc, wc);
        } while (n < 1);
        check (n > 0, "thread[%ld]: failed to poll cq", thread_id);

        if (ntohl(wc[0].imm_data) == MSG_CTL_START) {
            break;
        }
        post_srq_recv (msg_size, lkey, wc[i].wr_id, srq, (char *)wc[i].wr_id);
    }
    log ("thread[%ld]: ready to send", thread_id);

    /* pre-post sends */
    buf_offset = 0;
    debug ("buf_ptr = %"PRIx64"", (uint64_t)buf_ptr);
	for (j = 0; j < num_concurr_msgs; j++) {
        wr_id = get_wr_id();
        set_msg(buf_ptr,msg_size,wr_id%10);
	    // ret = post_send (msg_size, lkey, wr_id , (uint32_t)i, qp, buf_ptr);
	    ret = post_send_woimm (msg_size, lkey, wr_id , qp, buf_ptr);
	    check (ret == 0, "thread[%ld]: failed to post send", thread_id);
	    buf_offset = (buf_offset + msg_size) % buf_size;
	    buf_ptr = buf_base + buf_offset;
	}

    num_acked_peers = 0;
    while (stop != true) {
        /* poll cq */
        n = ibv_poll_cq (cq, num_wc, wc);
        if (n < 0) {
            check (0, "thread[%ld]: Failed to poll cq", thread_id);
        }

        for (i = 0; i < n; i++) {
            // if (unlikely(wc[i].status != IBV_WC_SUCCESS)) {
            //     check (0, "thread[%ld]: failed for opcode:%d failed status: %s",thread_id, wc[i].opcode,ibv_wc_status_str(wc[i].status));
            // }
            if(unlikely(++ops_count>=TOT_NUM_OPS)){
                gettimeofday (&end, NULL);
                stop = true;
                break;
            }
            
            // if(unlikely(ops_count % 100000 == 0)) log_message("run send %ld",ops_count);
            if (unlikely(ops_count == NUM_WARMING_UP_OPS)) gettimeofday (&start, NULL);

            wr_id = get_wr_id();
            // set_msg(buf_ptr,msg_size,wr_id%10);
            // ret = post_send (msg_size, lkey, wr_id, wr_id, qp, buf_ptr);
            ret = post_send_woimm (msg_size, lkey, wr_id, qp, buf_ptr);
            // check(ret==0,"Client send error");
            buf_offset = (buf_offset + msg_size) % buf_size;
            buf_ptr = buf_base + buf_offset;
            
        } /* loop through all wc */
    }

    /* dump statistics */
    duration   = (double)((end.tv_sec - start.tv_sec) * 1000000 + 
			  (end.tv_usec - start.tv_usec));
    uint64_t total = ((uint64_t)ops_count)*((uint64_t)msg_size);
    double tmp = 1.0*total;
    printf("total:%lf duration:%lf\n",tmp,duration);
    throughput = tmp / duration;
    // log ("thread[%ld]: throughput = %f (Mops/s)",  thread_id, throughput);
    printf("thread[%ld]: throughput = %f (MB/s)\n",  thread_id, throughput);
    log ("thread[%ld]: throughput = %f (MB/s)",  thread_id, throughput);

    free (wc);
    pthread_exit ((void *)0);

 error:
    if (wc != NULL) {
    	free (wc);
    }
    pthread_exit ((void *)-1);
}

int run_client ()
{
    int		ret	    = 0;
    long	num_threads = 1;
    long	i	    = 0;
    
    pthread_t	   *client_threads = NULL;
    pthread_attr_t  attr;
    void	   *status;

    log (LOG_SUB_HEADER, "Run Client");
    
    /* initialize threads */
    pthread_attr_init (&attr);
    pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_JOINABLE);

    client_threads = (pthread_t *) calloc (num_threads, sizeof(pthread_t));
    check (client_threads != NULL, "Failed to allocate client_threads.");

    for (i = 0; i < num_threads; i++) {
	ret = pthread_create (&client_threads[i], &attr, 
			      client_thread_func, (void *)i);
	check (ret == 0, "Failed to create client_thread[%ld]", i);
    }

    bool thread_ret_normally = true;
    for (i = 0; i < num_threads; i++) {
	ret = pthread_join (client_threads[i], &status);
	check (ret == 0, "Failed to join client_thread[%ld].", i);
	if ((long)status != 0) {
            thread_ret_normally = false;
            log ("thread[%ld]: failed to execute", i);
        }
    }

    if (thread_ret_normally == false) {
        goto error;
    }

    pthread_attr_destroy (&attr);
    free (client_threads);
    return 0;

 error:
    if (client_threads != NULL) {
        free (client_threads);
    }
    
    pthread_attr_destroy (&attr);
    return -1;
}
