#include <stdlib.h>
#include <stdbool.h>
#include <sys/time.h>
#include <thread>
#include <vector>

#include "debug.h"
#include "ib.h"
#include "setup_ib.h"
#include "config.h"
#include "server.h"
#include "RACE.h"
#include "NVM.h"

void server_thread (uint64_t id)
{
    int ret,n;
    char                *buf_ptr	= ib_res.ib_buf;
    uint32_t             lkey           = ib_res.mr->lkey;
    struct ibv_qp       *qp		= ib_res.qp;
    struct ibv_cq       *cq		= ib_res.cq;
    struct ibv_srq      *srq            = ib_res.srq;
    int                 num_wc  = 20;
    struct ibv_wc       *wc     = (struct ibv_wc *) calloc (num_wc, sizeof(struct ibv_wc));;

    // ib_buf is pre-allocated with the capacity to store all kv
    struct CCEH* cceh = (struct CCEH*)AllocPM(sizeof(struct CCEH));//Reserve a pointer at the begin of buf

    Init(cceh);
    PrintCCEH(cceh);

    //Send Start singal to all client
    sync_server_client();

    //Wait Finish Signal
    sync_server_client();
    
    PrintCCEH(cceh);
    Destroy(cceh);
}

int cceh_server ()
{
    uint64_t	num_threads = 1;
    std::vector<std::thread> client_threads;
    
    void	   *status;
    
    for (uint64_t i = 0; i < num_threads; i++) {
        client_threads.push_back(std::thread(server_thread,i));
    }

    for (uint64_t i = 0; i < num_threads; i++) {
        client_threads[i].join();
    }
    return 0;
}

