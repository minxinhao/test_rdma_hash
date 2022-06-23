#include <stdlib.h>
#include <stdbool.h>
#include <sys/time.h>
#include <thread>
#include <vector>
#include <math.h>
#include <string>
#include <chrono>
#include <thread>
#include <map>

#include "debug.h"
#include "config.h"
#include "setup_ib.h"
#include "ib.h"
#include "client.h"
#include "RACE.h"
#include "NVM.h"


uint64_t log_head;
uint64_t log_end;

void ReadCCEH(struct CCEH* cceh_cache){
    uint64_t wr_id = get_wr_id();
    //Read the Directory content
    int ret = post_read(sizeof(struct CCEH),ib_res.mr->lkey,wr_id,ib_res.rkey,ib_res.remote_addr,ib_res.qp,(char*)cceh_cache);
    if(ret)log_message("Fail to sycn cceh");
    wait_poll(wr_id);
}

int CheckDupKey(struct Bucket* buc,struct Slice* key,struct slot* tmp){
    int ret;
    uint64_t wr_id = get_wr_id();
    for(uint64_t i = 0 ; i < SLOT_PER_BUCKET ; i++){
        // log_message("FP:%d en:%d off:%d",tmp->fp==SLOT_FP(buc->slots[i]),tmp->len==SLOT_LEN(buc->slots[i]),tmp->offset!=SLOT_OFF(buc->slots[i]));
        if((SLOT_FP(buc->slots[i])==tmp->fp) && (SLOT_LEN(buc->slots[i])==tmp->len) 
            && (SLOT_OFF(buc->slots[i]) != tmp->offset) ){
            //Read Kv-block to cmp key
            char* tmp_key = (char*)AllocPM(SLOT_LEN(buc->slots[i]));
            ret = post_read(SLOT_LEN(buc->slots[i]),ib_res.mr->lkey,wr_id,ib_res.rkey,
                            log_end-SLOT_OFF(buc->slots[i]),ib_res.qp,tmp_key);
            wait_poll(wr_id);
            if(memcmp(key->data,tmp_key+sizeof(uint64_t)*2,key->len)==0){
                FreePM(tmp_key);
                return 1;
            }
            FreePM(tmp_key);
        }
    }
    return 0;
}

/*
return: 0-Success, 1-Bucket is full, 2-Duplicate key, -1-Inconsistent CCEH-Cache
*/
int BucketInsert(struct CCEH* cceh_cache,struct Bucket* bucket,uint64_t segloc,bool overflow_flag,
        uintptr_t buc_ptr,struct Slice* key,uint64_t pattern,uintptr_t kvblock_ptr, uint64_t block_len){
    int ret;
    // Check Cache consistency
    if(!overflow_flag){//只有main-bucket需要检查suffix和overflow
        if(bucket->local_depth != cceh_cache->dir[segloc].local_depth 
            && !CHECK_SUFFIX(bucket->suffix,segloc,cceh_cache->global_depth)){
            //mismatch
            log_message("Inconsistent CCEH Cache");
            ReadCCEH(cceh_cache);   
            return -1;
        }
    }

    struct slot* tmp = (struct slot*) AllocPM(sizeof(struct slot));
    tmp->fp = FP(pattern);
    tmp->len = block_len;
    tmp->offset = log_end - kvblock_ptr;

    //Find free slot and CAS the kvblock_ptr;
    uint64_t wr_id = get_wr_id();
    bool insert_flag = false;
    uint64_t slot_idx = 0;
    for( ; slot_idx < SLOT_PER_BUCKET ; slot_idx++){
        if(SLOT_FP(bucket->slots[slot_idx])==0){
            ret = post_cas(ib_res.mr->lkey,wr_id,ib_res.rkey,buc_ptr+sizeof(uint64_t)*(slot_idx+1),
                    ib_res.qp,(char*)tmp,Slot2U64(tmp),bucket->slots[slot_idx]);
            if(ret){log_message("Fail to CAS set slot ptr");}
            wait_poll(wr_id);
            U64Slot(*(uint64_t*)tmp,tmp);
            if(tmp->fp==0){ // CAS success
                insert_flag = true;
                break;
            }
        }
    }
    if(!insert_flag) { FreePM(tmp) ;return 1;}

    //Re-Read && check for duplicate key && Check Bucket Consistency
    wr_id = get_wr_id();
    struct Bucket* buc_data = (struct Bucket*)AllocPM(sizeof(struct Bucket));
    ret = post_read(sizeof(struct Bucket),ib_res.mr->lkey,wr_id,ib_res.rkey,
                    buc_ptr,ib_res.qp,(char*)(buc_data));
    if(ret) log_message("Fail to re-read bucket")
    wait_poll(wr_id);
    //Check CAS Result
    U64Slot(buc_data->slots[slot_idx],tmp);
    if(CheckDupKey(buc_data,key,tmp)){FreePM(tmp);FreePM(buc_data);return 2;}

    //Inconsistent bucket header implies split during insertion
    if(!CHECK_SUFFIX(bucket->suffix,segloc,cceh_cache->global_depth)){
        //Remove insert slot && reinsert
        tmp->fp = FP(pattern);
        tmp->len = block_len;
        tmp->offset = log_end - kvblock_ptr;
        wr_id = get_wr_id();
        ret = post_cas(ib_res.mr->lkey,wr_id,ib_res.rkey,buc_ptr+sizeof(uint64_t)*(slot_idx+1),
                    ib_res.qp,(char*)tmp,bucket->slots[slot_idx],Slot2U64(tmp));
        if(ret) log_message("Error in erase inserted slot")
        wait_poll(wr_id);
        U64Slot(*(uint64_t*)tmp,tmp);
        if(tmp->fp==FP(pattern) && tmp->len==block_len
            && (tmp->offset == log_end - kvblock_ptr)){ // CAS success
            //成功擦除，重新进行写入
            ReadCCEH(cceh_cache);  
            FreePM(tmp);FreePM(buc_data); 
            return -1;
        }else{
            //已经被split进行了转移或者被update/delete进行了更新，不用处理
        }
    }
    FreePM(tmp);FreePM(buc_data);
    return 0;
}


/*
return: 0-Success, 1-Bucket is full, 2-Duplicate key, -1-Inconsistent CCEH-Cache
*/
int BucketUpdate(struct CCEH* cceh_cache,struct Bucket* bucket,uint64_t segloc,bool overflow_flag,
        uintptr_t buc_ptr,struct Slice* key,uint64_t pattern,uintptr_t kvblock_ptr, uint64_t block_len){
    int ret;
    // Check Cache consistency
    if(!overflow_flag){//只有main-bucket需要检查suffix和overflow
        if(bucket->local_depth != cceh_cache->dir[segloc].local_depth 
          && !CHECK_SUFFIX(bucket->suffix,segloc,cceh_cache->global_depth)){
            //mismatch
            log_message("Inconsistent CCEH Cache");
            ReadCCEH(cceh_cache);   
            return -1;
        }
    }
    struct slot* update = (struct slot*) AllocPM(sizeof(struct slot));
    update->fp = FP(pattern);
    update->len = block_len;
    update->offset = log_end - kvblock_ptr;

    //Find Match Key. Check Fp in every slot
    for(uint64_t slot_idx = 0 ; slot_idx < SLOT_PER_BUCKET ; slot_idx++){
        if(SLOT_FP(bucket->slots[slot_idx]) == FP(pattern)){
            //Read KVBlock Ptr
            uint64_t wr_id = get_wr_id();
            struct KVBlock* kv_block = (struct KVBlock*)AllocPM(SLOT_LEN(bucket->slots[slot_idx]));
            ret = post_read(SLOT_LEN(bucket->slots[slot_idx]),ib_res.mr->lkey,wr_id,ib_res.rkey,
                        log_end - SLOT_OFF(bucket->slots[slot_idx]),ib_res.qp,(char*)kv_block);
            if(ret)log_message("Fail to read KvBlock");
            wait_poll(wr_id);

            //Cmp Key in kvblock with given key
            if(kv_block->k_len == key->len &&  memcmp(kv_block->data,key->data,key->len)==0){
                //CAS to update kvblock ptr
                struct slot* tmp = (struct slot*) AllocPM(sizeof(struct slot));
                ret = post_cas(ib_res.mr->lkey,wr_id,ib_res.rkey,buc_ptr+sizeof(uint64_t)*(slot_idx+1),
                ib_res.qp,(char*)tmp,Slot2U64(update),bucket->slots[slot_idx]);
                if(ret){log_message("Fail to CAS set slot ptr");}
                wait_poll(wr_id);
                if(*(uint64_t*)tmp==bucket->slots[slot_idx]){ // CAS success
                    //Re-Read && Check Bucket Consistency
                    wr_id = get_wr_id();
                    struct Bucket* buc_data = (struct Bucket*)AllocPM(sizeof(struct Bucket));
                    ret = post_read(sizeof(struct Bucket),ib_res.mr->lkey,wr_id,ib_res.rkey,
                                    buc_ptr,ib_res.qp,(char*)(buc_data));
                    if(ret) log_message("Fail to re-read bucket")
                    wait_poll(wr_id);
                    //Inconsistent bucket header implies split during insertion
                    if(!CHECK_SUFFIX(bucket->suffix,segloc,cceh_cache->global_depth)){
                        //Remove insert slot && reinsert
                        tmp->fp = FP(pattern);
                        tmp->len = block_len;
                        tmp->offset = log_end - kvblock_ptr;
                        wr_id = get_wr_id();
                        ret = post_cas(ib_res.mr->lkey,wr_id,ib_res.rkey,buc_ptr+sizeof(uint64_t)*(slot_idx+1),
                                    ib_res.qp,(char*)tmp,bucket->slots[slot_idx],Slot2U64(tmp));
                        if(ret) log_message("Error in erase inserted slot")
                        wait_poll(wr_id);
                        U64Slot(*(uint64_t*)tmp,tmp);
                        if(tmp->fp==FP(pattern) && tmp->len==block_len
                            && (tmp->offset == log_end - kvblock_ptr)){ // CAS success
                            //成功擦除，重新进行写入
                            ReadCCEH(cceh_cache); 
                            FreePM(tmp);FreePM(buc_data);FreePM(kv_block);
                            return -1;
                        }else{
                            //已经被split进行了转移或者被update/delete进行了更新，不用处理
                        }
                    }else{
                        //CAS Fail，其他的update/delete/split成功，直接返回不用处理
                        FreePM(kv_block);
                        return 0; 
                    }
                }else{
                    //CAS Fail，其他的update/delete/split成功，直接返回不用处理
                    FreePM(kv_block);
                    return 0;
                }
            }
            FreePM(kv_block);
        }
    }
    return 1;
 
}

/*
* Insert within main_buc and over_buc
* return: 0-Success, 1-Bucket is full, 2-Duplicate key, -1-Inconsistent CCEH-Cache
*/
int BucketGroupInsert(struct CCEH* cceh_cache,uint64_t buc_idx,struct Bucket* buc_data,uint64_t buc_ptr,
    struct Slice* key,uint64_t pattern,uint64_t segloc,uintptr_t kvblock_ptr, uint64_t block_len,bool update_flag){
    int ret;
    struct Bucket* main_buc = MAIN_BUCKET(buc_idx,buc_data);
    struct Bucket* over_buc = OVER_BUCKET(buc_idx,buc_data);
    uint64_t main_buc_ptr = MAIN_BUCKET_PTR(buc_idx,buc_ptr);
    uint64_t over_buc_ptr = OVER_BUCKET_PTR(buc_idx,buc_ptr);
    if(update_flag){
        ret = BucketUpdate(cceh_cache,main_buc,segloc,false,main_buc_ptr,
                key,pattern,kvblock_ptr,block_len);
    }else{
        ret = BucketInsert(cceh_cache,main_buc,segloc,false,main_buc_ptr,
                key,pattern,kvblock_ptr,block_len);
    }
    if(ret==1){
        //Main-bucket is full, try to Insert OverBucket
        if(update_flag){
            ret = BucketUpdate(cceh_cache,over_buc,segloc,true,over_buc_ptr,
                key,pattern,kvblock_ptr,block_len);
        }else{
            ret = BucketInsert(cceh_cache,over_buc,segloc,true,over_buc_ptr,
                key,pattern,kvblock_ptr,block_len);
        }        
        return ret;
    }
    return ret;
}



void GlobalUnlock(struct CCEH* cceh_cache){
    log_message("====Global Split End======");
    int ret;
    uint64_t wr_id ;
    uint64_t* lock = (uint64_t*)AllocPM(sizeof(uint64_t));
    *lock = GLOBAL_BIT_MASK;
    uint64_t old_lock , new_lock;

    //Set global split bit
    old_lock = *lock;
    new_lock = 0llu;
    wr_id = get_wr_id();
    ret = post_cas(ib_res.mr->lkey,wr_id,ib_res.rkey,ib_res.remote_addr,
                ib_res.qp,(char*)lock,new_lock,old_lock);
    if(ret)log_message("Error in cas to lock directory")
    wait_poll(wr_id);
    // log_message("old_lock:%lx lock:%lx",old_lock,*lock);
    if(( *lock != old_lock)){
        log_message("Another split to conflict global-unlock")
    }
}

/*
*   global split lock：
*       设置global split bit为1，阻止后续split；
*       检查local-split count是否为零；不为零等待完成；
*   return: 0-success, 1-split conflict
*/
int GlobalLock(struct CCEH* cceh_cache){
    log_message("====Global Split======");
    int ret;
    uint64_t wr_id ;
    uint64_t* lock = (uint64_t*)AllocPM(sizeof(uint64_t));
    *lock = cceh_cache->resize_lock;
    uint64_t old_lock , new_lock;

    //Set global split bit
    while(true){
        old_lock = *lock;
        if(GLOBAL_BIT(old_lock)){
            //global split bit is set
            FreePM(lock);
            return 1;
        }
        new_lock = old_lock|GLOBAL_BIT_MASK;
        wr_id = get_wr_id();
        ret = post_cas(ib_res.mr->lkey,wr_id,ib_res.rkey,ib_res.remote_addr,
                    ib_res.qp,(char*)lock,new_lock,old_lock);
        if(ret)log_message("Error in cas to lock directory")
        wait_poll(wr_id);
        // log_message("old_lock:%lx lock:%lx",old_lock,*lock);
        if(( *lock == old_lock)){
            //Success to lock directory
            break;
        }
    }
    

    //wait local-splits to complete
    while((*lock)&(!GLOBAL_BIT_MASK)){
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        wr_id = get_wr_id();
        ret = post_read(sizeof(uint64_t),ib_res.mr->lkey,wr_id,ib_res.rkey,
                ib_res.remote_addr,ib_res.qp,(char*)lock);
        if(ret)log_message("Fail to read local-split count")
        wait_poll(wr_id);
    }

    //Read CCEH to get the newest global depth and local-depth
    ReadCCEH(cceh_cache);
    FreePM(lock);
    log_message("Global split lock success");
    return 0;
}


void LocalUnlock(struct CCEH* cceh_cache,uint64_t seg_loc,uint64_t local_depth){
    log_message("====Local Split End======");
    int ret;
    uint64_t wr_id ;
    uint64_t* resize_lock = (uint64_t*)AllocPM(sizeof(uint64_t));
    *resize_lock = cceh_cache->resize_lock;
    uint64_t old_lock ,new_lock ;

    //Relase lock in the first old_seg && new_seg
    ReadCCEH(cceh_cache);
    //old_seg
    uint64_t first_seg_loc = seg_loc & ((1llu<<(local_depth-1))); 
    *resize_lock = cceh_cache->dir[first_seg_loc].local_split_lock;
    wr_id = get_wr_id();
    ret = post_cas(ib_res.mr->lkey,wr_id,ib_res.rkey,
                ib_res.remote_addr+2*sizeof(uint64_t)+first_seg_loc*sizeof(struct DirEntry),
                ib_res.qp,(char*)resize_lock,0,1);
    if(ret)log_message("Error in release lock in old_seg")
    wait_poll(wr_id);
    if((*resize_lock)!=1){
        log_message("Another split to old_seg conflict local-unlock")
    }

    //new seg
    first_seg_loc = first_seg_loc | (1<<local_depth);
    *resize_lock = cceh_cache->dir[first_seg_loc].local_split_lock;
    wr_id = get_wr_id();
    ret = post_cas(ib_res.mr->lkey,wr_id,ib_res.rkey,
                ib_res.remote_addr+2*sizeof(uint64_t)+first_seg_loc*sizeof(struct DirEntry),
                ib_res.qp,(char*)resize_lock,0,1);
    if(ret)log_message("Error in release lock in new_seg")
    wait_poll(wr_id);
    if((*resize_lock)!=1){
        log_message("Another split to new_seg conflict local-unlock")
    }



    //Decrease local-split cnt
    wr_id = get_wr_id();
    ret = post_add(ib_res.mr->lkey,wr_id,ib_res.rkey,ib_res.remote_addr,
                ib_res.qp,(char*)resize_lock,-1);
    if(ret)log_message("Error in Decrease local-split cnt")
    wait_poll(wr_id);
    FreePM(resize_lock);
}

/*
*   local-split：
*       每次local split会检查global split是否为1，如果是等待global split完成；
*       每次local split会增加local-split count，local split结束后会减一；
*       lock住共享seg_ptr的第一个directory entry.
*   return: 0-success, 1-split conflict
*/
int LocalLock(struct CCEH* cceh_cache,uint64_t seg_loc,uint64_t local_depth){
    log_message("====Local Split======");
    int ret;
    uint64_t wr_id ;
    uint64_t* resize_lock = (uint64_t*)AllocPM(sizeof(uint64_t));
    *resize_lock = cceh_cache->resize_lock;
    uint64_t old_lock ,new_lock ;

    //Add local-split cnt
    while(true){
        old_lock = *resize_lock;
        if(GLOBAL_BIT(old_lock)){
            //global split bit is set
            FreePM(resize_lock);
            return 1;
        }
        new_lock = old_lock+1;
        wr_id = get_wr_id();
        ret = post_cas(ib_res.mr->lkey,wr_id,ib_res.rkey,ib_res.remote_addr,
                    ib_res.qp,(char*)resize_lock,new_lock,old_lock);
        if(ret)log_message("Error in cas to lock directory")
        wait_poll(wr_id);
        if((*resize_lock)==old_lock){
            break;
        }
    }

    //Read CCEH to get the newest global depth and local-depth
    ReadCCEH(cceh_cache);

    //Lock first directory entry sharing this segment
    uint64_t first_seg_loc = seg_loc & ((1llu<<(local_depth-1)));
    *resize_lock = cceh_cache->dir[first_seg_loc].local_split_lock;
    while(true){
        if(*resize_lock){
            // Direntry is already locked. Decrease the local-split count
            wr_id = get_wr_id();
            ret = post_add(ib_res.mr->lkey,wr_id,ib_res.rkey,ib_res.remote_addr,
                        ib_res.qp,(char*)resize_lock,-1);
            if(ret)log_message("Error in cas to lock directory")
            wait_poll(wr_id);
            FreePM(resize_lock);
            return 1;
        }
        // lock Direntry
        wr_id = get_wr_id();
        ret = post_cas(ib_res.mr->lkey,wr_id,ib_res.rkey,
                    ib_res.remote_addr+2*sizeof(uint64_t)+first_seg_loc*sizeof(struct DirEntry),
                    ib_res.qp,(char*)resize_lock,1,0);
        if(ret)log_message("Error in cas to lock directory")
        wait_poll(wr_id);
        if((*resize_lock)==0){
            break;
        }
    }

    FreePM(resize_lock);
    log_message("Lock split lock success");
    return 0;
}


// Used in MoveData
// return : 0-success to write slot into new_seg at bucidx
//          1-invalid bucidx
int SetSlot(uint64_t buc_ptr,std::map<uint64_t,uint64_t>& bucket_free,uint64_t bucidx,
            uint64_t slot,char* tmp){
    uint64_t slot_idx =  bucket_free[bucidx];
    uint64_t wr_id ;
    int ret;
    while(slot_idx<SLOT_PER_BUCKET){
        wr_id = get_wr_id();
        ret = post_cas(ib_res.mr->lkey,wr_id,ib_res.rkey,buc_ptr+sizeof(uint64_t)*(slot_idx+1),
                    ib_res.qp,tmp,slot,0);
        if(ret)log_message("Fail to CAS set slot ptr");
        wait_poll(wr_id);
        if((*(uint64_t*)tmp)==0) {
            bucket_free[bucidx]=slot_idx+1;
            return 0;
        }
        log_message("Fail to CAS set slot ptr");
        slot_idx++;
    }
    return 1;
}


void MoveData(struct CCEH* cceh_cache,uint64_t old_seg_ptr,uint64_t new_seg_ptr,
                struct Segment* seg,struct Segment* new_seg){
    int ret;
    uint64_t wr_id ;
    struct Bucket* cur_buc;
    uint64_t pattern_1,pattern_2,suffix;
    uint64_t buc_ptr;
    struct KVBlock* kv_block;
    uint64_t* tmp = (uint64_t*) AllocPM(sizeof(uint64_t));
    std::map<uint64_t,uint64_t> bucket_free; //records the first free slot_idx of every bucket
    for(uint64_t i = 0 ; i < BUCKET_PER_SEGMENT*3 ; i++) bucket_free[i]=0;

    log_message("old_seg_ptr:%lx",old_seg_ptr);
    for(uint64_t i = 0 ; i < BUCKET_PER_SEGMENT*3 ; i++){
        buc_ptr = old_seg_ptr+i*sizeof(struct Bucket);
        cur_buc = &seg->buckets[i];
        cur_buc->local_depth = new_seg->buckets[0].local_depth;
        cur_buc->suffix = new_seg->buckets[0].suffix;
        wr_id = get_wr_id();
        //Update local_depth&suffix
        ret = post_write(2*sizeof(uint64_t),ib_res.mr->lkey,wr_id,ib_res.rkey,
                        old_seg_ptr+i*sizeof(struct Bucket),ib_res.qp,(char*)cur_buc);
        if(ret)log_message("fail to updaet bucket depth&suffix");
        wait_poll(wr_id);

        for(uint64_t slot_idx = 0 ; slot_idx < SLOT_PER_BUCKET ; slot_idx++){
            if(SLOT_FP(cur_buc->slots[slot_idx])==0)continue;

            char* tmp_key = (char*)AllocPM(SLOT_LEN(cur_buc->slots[slot_idx]));
            ret = post_read(SLOT_LEN(cur_buc->slots[slot_idx]),ib_res.mr->lkey,wr_id,ib_res.rkey,
                            log_end-SLOT_OFF(cur_buc->slots[slot_idx]),ib_res.qp,tmp_key);
            if(ret)log_message("Fail to Read key in old_seg");
            wait_poll(wr_id);
            kv_block = (struct KVBlock*)tmp_key;
            pattern_1 = hash_1(kv_block->data,kv_block->k_len);
            suffix = SEGMENT_LOC(pattern_1,cceh_cache->global_depth);
            // log_message("suffix:%lx new_suffix:%x",suffix,new_seg->buckets[0].suffix);
            if(suffix==new_seg->buckets[0].suffix){
                //Find free slot in two bucketgroup
                pattern_2 = hash_2(kv_block->data,kv_block->k_len);
                uint64_t bucidx_1 = BUCKET_LOC(pattern_1);
                uint64_t bucptr_1 = new_seg_ptr+BUCKET_OFF(bucidx_1);
                uint64_t main_buc_ptr1 = MAIN_BUCKET_PTR(bucidx_1,bucptr_1);
                uint64_t over_buc_ptr1 = OVER_BUCKET_PTR(bucidx_1,bucptr_1);
                uint64_t bucidx_2 = BUCKET_LOC(pattern_2);
                uint64_t bucptr_2 = new_seg_ptr+BUCKET_OFF(bucidx_2);
                uint64_t main_buc_ptr2 = MAIN_BUCKET_PTR(bucidx_2,bucptr_2);
                uint64_t over_buc_ptr2 = OVER_BUCKET_PTR(bucidx_2,bucptr_2);
                log_message("bucidx_1:%ld bucidx_2:%ld",bucidx_1,bucidx_2);
                log_message("bucptr_1:%lx bucptr_2:%lx",bucptr_1,bucptr_2);


                //依次尝试Bucket 1，OverBuc 1，Bucket 2，OverBuc 2
                if(SetSlot(main_buc_ptr1,bucket_free,TRUE_BUC_IDX(bucidx_1),cur_buc->slots[slot_idx],(char*)tmp) 
                   && SetSlot(over_buc_ptr1,bucket_free,TRUE_OVER_IDX(bucidx_1),cur_buc->slots[slot_idx],(char*)tmp)
                   && SetSlot(main_buc_ptr2,bucket_free,TRUE_BUC_IDX(bucidx_2),cur_buc->slots[slot_idx],(char*)tmp)
                   && SetSlot(over_buc_ptr2,bucket_free,TRUE_OVER_IDX(bucidx_2),cur_buc->slots[slot_idx],(char*)tmp)){
                    log_message("No free bucket in new_seg");
                }

                //CAS slot in old seg to zero
                ret = post_cas(ib_res.mr->lkey,wr_id,ib_res.rkey,buc_ptr+sizeof(uint64_t)*(slot_idx+1),
                    ib_res.qp,(char*)tmp,0,cur_buc->slots[slot_idx]);
                if(ret){log_message("Fail to CAS set slot ptr");}
                if(*tmp!=cur_buc->slots[slot_idx]){
                    //Move fail
                    //也不影响，只要是这里被的旧slot被删除了就行
                    //只有可能是并发的update导致的
                }
                wait_poll(wr_id);
            }

            FreePM(tmp_key);
        }
    }
    FreePM(tmp);
}


/*
*   使用global和local lock来控制resize进行；最高位是global split bit，后续为local-split count
*   local-split：
*       每次local split会检查global split是否为1，如果是等待global split完成；
*       每次local split会增加local-split count，local split结束后会减一；
*       lock住共享seg_ptr的第一个directory entry.
*   global split：
*       设置global split bit为1，阻止后续split；
*       检查local-split count是否为零；不为零等待完成；
*   return: 0-success, 1-split conflict
*/
int CCEHSplit(struct CCEH* cceh_cache,struct Bucket* bucket, uint64_t seg_loc,uintptr_t seg_ptr , 
    uint64_t local_depth,bool global_flag){
    //Lock 
    if(global_flag){
        if(GlobalLock(cceh_cache)){
            log_message("Fail to lock global split");
            return 1;
        }
    }else{
        if(LocalLock(cceh_cache,seg_loc,local_depth)){
            log_message("Fail to lock local split");
            return 1;
        }
    }

    //Allocate New Seg and Init header && write to server
    struct Segment* new_seg=(struct Segment*)AllocPM(sizeof(struct Segment));
    log_head -= sizeof(struct Segment);    // TODO:这里改成CAS client_logs
    log_head = (log_head>>3)<<3; //按八字节对齐
    uint64_t new_seg_ptr = log_head;
    uint64_t new_seg_loc = seg_loc | (1 << local_depth);
    log_message("seg_ptr:%lx seg_loc:%ld",seg_ptr,seg_loc);
    log_message("new_seg_ptr:%lx new_seg_loc:%ld",new_seg_ptr,new_seg_loc);
    for(uint64_t i = 0 ; i < BUCKET_PER_SEGMENT*3 ; i++){
        new_seg->buckets[i].local_depth = local_depth+1;
        new_seg->buckets[i].suffix = new_seg_loc;
    }
    uint64_t wr_id = get_wr_id();
    int ret = post_write(sizeof(struct Segment),ib_res.mr->lkey,wr_id,ib_res.rkey,new_seg_ptr,
                    ib_res.qp,(char*)new_seg);
    if(ret)log_message("Error in writing new_seg to server")             
    wait_poll(wr_id);

    //Edit Directory pointer
    /* 因为使用了MSB和提前分配充足空间的Directory，所以可以直接往后增加Directory Entry*/
    log_message("Edit Directory")
    if(global_flag){
        //Global split: Set the second half of directory
        uint64_t half_size = 1<<cceh_cache->global_depth;
        memcpy(cceh_cache->dir+half_size,cceh_cache->dir,half_size*sizeof(struct DirEntry));
        cceh_cache->dir[new_seg_loc].local_depth = local_depth+1;
        cceh_cache->dir[new_seg_loc].seg_ptr = new_seg_ptr;
        cceh_cache->global_depth++;
        wr_id = get_wr_id();
        ret = post_write(half_size*sizeof(struct DirEntry),ib_res.mr->lkey,wr_id,ib_res.rkey,
                        ib_res.remote_addr+2*sizeof(uint64_t)+half_size*sizeof(struct DirEntry),
                        ib_res.qp,(char*)(cceh_cache->dir+half_size));
        if(ret)log_message("Global Split fail to edit server's directory");
        wait_poll(wr_id);
        wr_id = get_wr_id();
        ret = post_write(sizeof(uint64_t),ib_res.mr->lkey,wr_id,ib_res.rkey,
                        ib_res.remote_addr+sizeof(uint64_t),
                        ib_res.qp,(char*)(&(cceh_cache->global_depth)));
        if(ret)log_message("Fail to write global depth  to server");
        wait_poll(wr_id);
    }else{
        //Local split: Edit all directory share this seg_ptr
        //初始pattern是seg_loc & (1<<(local_depth-1))
        //local_depth+1后，local_depth为1的direntry被更新为new_seg_ptr
        // ex. 000, 010, 100, 110 都共享同一个seg；local-split后，000，100，共享同一个，010，110共享同一个
        log_message("global_depth:%ld local_depth:%ld",cceh_cache->global_depth,local_depth);
        uint64_t first_seg_loc = seg_loc & ((1<<(local_depth-1)));
        first_seg_loc = first_seg_loc | (1<<local_depth);
        log_message("new_seg_pattern:%lx",first_seg_loc);
        uint64_t stride = (1llu)<<(cceh_cache->global_depth-local_depth-1);
        uint64_t cur_seg_loc;
        log_message("stride:%lu",stride);
        
        //We should lock the new-segment as well.
        cceh_cache->dir[first_seg_loc].local_split_lock = 1;
        for(uint64_t i = 0 ; i < stride ; i++){
            cur_seg_loc = (i<<(local_depth+1))|first_seg_loc;
            // log_message("cur_seg_loc:%lx",cur_seg_loc);
            cceh_cache->dir[cur_seg_loc].seg_ptr = new_seg_ptr;
            cceh_cache->dir[cur_seg_loc].local_depth = local_depth+1;
        }
        wr_id = get_wr_id();
        //TODO：应该改成CAS？
        ret = post_write_dir(ib_res.mr->lkey,wr_id,ib_res.rkey,ib_res.remote_addr,ib_res.qp,
            cceh_cache,local_depth,first_seg_loc,stride);
        if(ret)log_message("Local Split fail to edit server's directory");
        wait_poll(wr_id);
    }
    
    //Move Data
    log_message("Move Data");
    struct Segment* old_seg=(struct Segment*)AllocPM(sizeof(struct Segment));
    wr_id = get_wr_id();
    ret = post_read(sizeof(struct Segment),ib_res.mr->lkey,wr_id,ib_res.rkey,
                            seg_ptr,ib_res.qp,(char*)old_seg);
    if(ret)log_message("Fail to read old seg");
    wait_poll(wr_id);
    MoveData(cceh_cache,seg_ptr,new_seg_ptr,old_seg,new_seg);

    //Unlock
    if(global_flag){
        //Set Global split bit to zero
        GlobalUnlock(cceh_cache);
    }else{
        //Release lock in DirEntry and decrease local-split count
        LocalUnlock(cceh_cache,seg_loc,local_depth);
    }
    

    FreePM(old_seg);
    FreePM(new_seg);
    return 0;
}


void InsertCCEH(struct CCEH* cceh_cache,struct Slice* key, struct Slice* value,bool update_flag){
    //Calculate pattern of key
    uint64_t pattern_1,pattern_2;
    pattern_1 = hash_1(key->data,key->len);
    pattern_2 = hash_2(key->data,key->len);
    
Retry: 
    //Read Segment Ptr From CCEH_Cache
    uint64_t segloc;
    uintptr_t segptr;
    segloc = SEGMENT_LOC(pattern_1,cceh_cache->global_depth);
    segptr = cceh_cache->dir[segloc].seg_ptr;
    // log_message("segloc_1:%lx segloc_2:%lx",segloc_1,segloc_2);
    // log_message("segptr_1:%lx segptr_2:%lx",segptr_1,segptr_2);

    //Compute two bucket location
    uint64_t bucidx_1, bucidx_2; //calculate bucket idx for each key
    uintptr_t bucptr_1,bucptr_2;
    bucidx_1 = BUCKET_LOC(pattern_1);
    bucidx_2 = BUCKET_LOC(pattern_2);
    bucptr_1 = segptr+BUCKET_OFF(bucidx_1);
    bucptr_2 = segptr+BUCKET_OFF(bucidx_2);
    // log_message("bucidx_1:%ld bucidx_2:%ld",bucidx_1,bucidx_2);
    // log_message("bucptr_1:%lx bucptr_2:%lx",bucptr_1,bucptr_2);


    //Doorbell Read && Write KV-Data
    uint64_t wr_id_read = get_wr_id();
    struct Bucket* buc_data =(struct Bucket*) AllocPM(4*sizeof(struct Bucket));
    int ret = post_read2(2*sizeof(struct Bucket),ib_res.mr->lkey,wr_id_read,ib_res.rkey,
                    bucptr_1,bucptr_2,ib_res.qp,(char*)buc_data);
    if(ret) log_message("Fail to doorbell read 2 bucket ret:%d",ret);
    // wait_poll(wr_id_read);

    struct KVBlock* kv_block = InitKVBlock(key,value);
    uint64_t kvblock_len = key->len+value->len+sizeof(uint64_t)*2;
    log_head -= kvblock_len;    // TODO:这里改成CAS client_logs
    uint64_t kvblock_ptr = log_head;
    log_message("kvblock_ptr:%lx",kvblock_ptr);
    uint64_t wr_id_write = get_wr_id(); 
    ret = post_write(kvblock_len,ib_res.mr->lkey,wr_id_write,ib_res.rkey,
                    kvblock_ptr,ib_res.qp,(char*)(kv_block)); 
    if(ret) log_message("Fail to write kv-block:%d",ret);
    // wait_poll(wr_id_write);
    DestoryKVBlock(kv_block);

    //wait for doorbell read and write to complete
    uint64_t res,total = 0;
    struct ibv_wc wc; 
    while(total < 2){
        res = ibv_poll_cq(ib_res.cq, 1, &wc); 
        // total+=res;//这样的写法，在并发情况下应该有问题，应该根据wr_id来判断？
        if(wc.wr_id == wr_id_read || wc.wr_id == wr_id_write)total++;
    }

    // for(uint64_t i = 0 ; i < 4 ; i++)
    //     log_message("buc: local-depth:%x suffix:%x",buc_data[i].local_depth,buc_data[i].suffix);

    // 两个bucket依次进行
    ret = BucketGroupInsert(cceh_cache,bucidx_1,buc_data,bucptr_1,key,pattern_1,
                            segloc,kvblock_ptr,kvblock_len,update_flag);
    if(ret==0){
        return;
    }
    else if(ret==-1){
        //Re-read CCEH cache;
        goto Retry;
    }else if(ret==1){
        //BucketGroup1 is full, try to insert into bucket group 2
        log_message("BucketGroup1 is full");
    }else if(ret==2){
        log_message("duplicate key");
        return;
    }

    ret = BucketGroupInsert(cceh_cache,bucidx_2,buc_data+2,bucptr_2,key,pattern_2,
                            segloc,kvblock_ptr,kvblock_len,update_flag);
    if(ret==0){
        return;
    }
    else if(ret==-1){
        //Re-read CCEH cache;
        goto Retry;
    }else if(ret==1){
        log_message("BucketGroup2 is full,split");
        //split
        if((buc_data+2)->local_depth == cceh_cache->global_depth){
            //Global split
            CCEHSplit(cceh_cache,buc_data+2,segloc,segptr,(buc_data+2)->local_depth,true);
        }else{
            //Local split
            CCEHSplit(cceh_cache,buc_data+2,segloc,segptr,(buc_data+2)->local_depth,false);
        }
        ReadCCEH(cceh_cache);
        goto Retry;
    }else if(ret==2){
        log_message("duplicate key");
    }
}


/*
return: 0-Success, 1-No match key in inputting buc,  -1-Inconsistent CCEH-Cache
*/
int BucketSearch(struct CCEH* cceh_cache,struct Bucket* bucket,uint64_t segloc,bool overflow_flag,
    uint64_t buc_ptr,struct Slice* key,uint64_t pattern,struct Slice* value, bool del_flag){
    int ret;
    // Check Cache consistency
    if(!overflow_flag){//只有main-bucket需要检查suffix和overflow
        if(bucket->local_depth != cceh_cache->dir[segloc].local_depth 
           && !CHECK_SUFFIX(bucket->suffix,segloc,cceh_cache->global_depth)){
            //mismatch
            log_message("Inconsistent CCEH Cache");
            ReadCCEH(cceh_cache);   
            return -1;
        }
    }

    // Check Fp in every slot
    for(uint64_t slot_idx = 0 ; slot_idx < SLOT_PER_BUCKET ; slot_idx++){
        log_message("Slot: fp:%lx len:%ld offset:%lx",SLOT_FP(bucket->slots[slot_idx]),SLOT_LEN(bucket->slots[slot_idx]),SLOT_OFF(bucket->slots[slot_idx]));
        if(SLOT_FP(bucket->slots[slot_idx]) == FP(pattern)){
            //Read KVBlock Ptr
            uint64_t wr_id = get_wr_id();
            struct KVBlock* kv_block = (struct KVBlock*)AllocPM(SLOT_LEN(bucket->slots[slot_idx]));
            ret = post_read(SLOT_LEN(bucket->slots[slot_idx]),ib_res.mr->lkey,wr_id,ib_res.rkey,
                        log_end - SLOT_OFF(bucket->slots[slot_idx]),ib_res.qp,(char*)kv_block);
            if(ret)log_message("Fail to read KvBlock");
            wait_poll(wr_id);
            log_message("kv_block:k_len:%ld,v_len:%ld,key-value:%s",kv_block->k_len,kv_block->v_len,kv_block->data);
            
            //Cmp Key in kvblock with given key
            if(kv_block->k_len == key->len &&  memcmp(kv_block->data,key->data,key->len)==0){
                //Set Value
                if(!del_flag){
                    value->len = kv_block->v_len;
                    value->data = kv_block->data+kv_block->k_len;
                }else{
                    //post cas to delete slot
            
                    uint64_t* tmp = (uint64_t*) AllocPM(sizeof(uint64_t));
                    ret = post_cas(ib_res.mr->lkey,wr_id,ib_res.rkey,buc_ptr+sizeof(uint64_t)*(slot_idx+1),
                    ib_res.qp,(char*)tmp,0,bucket->slots[slot_idx]);
                    if(ret)log_message("Fail to CAS set slot ptr");
                    log_message("delete-cas return");
                    log_message("Slot: fp:%lx len:%ld offset:%lx",SLOT_FP(*tmp),SLOT_LEN(*tmp),SLOT_OFF(*tmp));
                    wait_poll(wr_id);
                    FreePM(tmp);
                    //Delete不用处理和并发insert，update，split的逻辑，成功和失败后都直接返回
                }
                FreePM(kv_block);
                return 0;
            }
            FreePM(kv_block);
        }
    }

    //没有读到，Re-Read Bucket看是否因为split发生了get miss
    //Re-Read && Check Bucket Consistency
    uint64_t  wr_id = get_wr_id();
    struct Bucket* buc_data = (struct Bucket*)AllocPM(sizeof(struct Bucket));
    ret = post_read(sizeof(struct Bucket),ib_res.mr->lkey,wr_id,ib_res.rkey,
                    buc_ptr,ib_res.qp,(char*)(buc_data));
    if(ret) log_message("Fail to re-read bucket")
    wait_poll(wr_id);
    //Inconsistent bucket header implies split during searching
    if(!CHECK_SUFFIX(bucket->suffix,segloc,cceh_cache->global_depth)){
        //Remove insert slot && reinsert
        ReadCCEH(cceh_cache); 
        return -1;
    }

    return 1;
  
}

/*
* Search Key within main_buc and over_buc
* return: 0-Success, 1-No match key in main_buc and over_buc ,-1-Inconsistent CCEH-Cache
*/
int BucketGroupSearch(struct CCEH* cceh_cache,uint64_t buc_idx,struct Bucket* buc_data,struct Slice* key,
    uint64_t buc_ptr,uint64_t pattern,uint64_t segloc,struct Slice* value, bool del_flag){
    int ret;
    struct Bucket* main_buc = MAIN_BUCKET(buc_idx,buc_data);
    struct Bucket* over_buc = OVER_BUCKET(buc_idx,buc_data);
    uint64_t main_buc_ptr1 = MAIN_BUCKET_PTR(buc_idx,buc_ptr);
    uint64_t over_buc_ptr1 = OVER_BUCKET_PTR(buc_idx,buc_ptr);
    ret = BucketSearch(cceh_cache,main_buc,segloc,false,main_buc_ptr1,key,pattern,value,del_flag);
    if(ret==1){
        //No match key in Main Bucket, find in over bucket
        ret = BucketSearch(cceh_cache,over_buc,segloc,true,over_buc_ptr1,key,pattern,value,del_flag);
        return ret;
    }
    return ret;
}

void SearchCCEH(struct CCEH* cceh_cache,struct Slice* key, struct Slice* value, bool del_flag){
    //Calculate pattern of key
    uint64_t pattern_1,pattern_2;
    pattern_1 = hash_1(key->data,key->len);
    pattern_2 = hash_2(key->data,key->len);
    
Retry: 
    //Read Segment Ptr From CCEH_Cache
    uint64_t segloc;
    uintptr_t segptr;
    segloc = SEGMENT_LOC(pattern_1,cceh_cache->global_depth);
    segptr = cceh_cache->dir[segloc].seg_ptr;
    log_message("segloc:%lx segptr:%lx",segloc,segptr);

    //Compute two bucket location
    uint64_t bucidx_1, bucidx_2; //calculate bucket idx for each key
    uintptr_t bucptr_1,bucptr_2;
    bucidx_1 = BUCKET_LOC(pattern_1);
    bucidx_2 = BUCKET_LOC(pattern_2);
    bucptr_1 = segptr+BUCKET_OFF(bucidx_1);
    bucptr_2 = segptr+BUCKET_OFF(bucidx_2);
    log_message("bucidx_1:%ld bucidx_2:%ld",bucidx_1,bucidx_2);
    log_message("bucptr_1:%lx bucptr_2:%lx",bucptr_1,bucptr_2);

    //Read 2 Bucket
    uint64_t wr_id_read = get_wr_id();
    struct Bucket* buc_data =(struct Bucket*) AllocPM(4*sizeof(struct Bucket));
    int ret = post_read2(2*sizeof(struct Bucket),ib_res.mr->lkey,wr_id_read,ib_res.rkey,
                    bucptr_1,bucptr_2,ib_res.qp,(char*)buc_data);
    if(ret) log_message("Fail to doorbell read 2 bucket ret:%d",ret);
    wait_poll(wr_id_read);

    //Check FP in Main Bucket && Over Bucket && 2-choice 
    ret = BucketGroupSearch(cceh_cache,bucidx_1,buc_data,key,bucptr_1,pattern_1,segloc,value,del_flag);
    if(ret==0) return;
    else if(ret==-1) goto  Retry;
    else if(ret==1){
        log_message("No match key in bucket group 1");
    }

    ret = BucketGroupSearch(cceh_cache,bucidx_2,buc_data+2,key,bucptr_2,pattern_2,segloc,value,del_flag);
    if(ret==0) return;
    else if(ret==-1) goto  Retry;
    else if(ret==1){
        log_message("No match key in bucket group 2");
    }

}


void client_thread_func (uint64_t id)
{
    // 每个client从server端buf的尾部写入kv-block；每个client占据一定的间隔(2GB for example)，避免复杂的log管理；
    log_head = (uint64_t)ib_res.remote_addr + ib_res.ib_buf_size;
    log_end = log_head;
    //Wait start signal
    sync_server_client();

    struct CCEH* cceh_cache= (struct CCEH*)AllocPM(sizeof(struct CCEH));//Reserve a pointer at the begin of buf
    ReadCCEH(cceh_cache);
    log_message("Global Depth:%lu",cceh_cache->global_depth);

    std::string key_str="1234567";
    struct Slice key={
        .len = key_str.length(),
        .data = (char*)key_str.data()
    };
    std::string val_str="11111111";
    struct Slice value={
        .len = val_str.length(),
        .data = (char*)val_str.data()
    };
    
    log_message("-------Check Insert && Get----");
    InsertCCEH(cceh_cache,&key,&value,false);
    SearchCCEH(cceh_cache,&key,&value,false);

    log_message("-------check duplicate key-----");
    for(uint64_t i = 0 ; i < 4*SLOT_PER_BUCKET-1 ; i++){
        InsertCCEH(cceh_cache,&key,&value,false);
    }
    SearchCCEH(cceh_cache,&key,&value,false);


    log_message("-------Check Delete---------")
    SearchCCEH(cceh_cache,&key,&value,true);
    SearchCCEH(cceh_cache,&key,&value,false);

    log_message("--------Check Update--------")
    std::string val_str_2="2222222";
    struct Slice value_2={
        .len = val_str_2.length(),
        .data = (char*)val_str_2.data()
    };
    InsertCCEH(cceh_cache,&key,&value_2,false);
    SearchCCEH(cceh_cache,&key,&value_2,false);

    std::string val_str_3="3333";
    struct Slice value_3={
        .len = val_str_3.length(),
        .data = (char*)val_str_3.data()
    };
    InsertCCEH(cceh_cache,&key,&value_3,true);
    SearchCCEH(cceh_cache,&key,&value_3,false);

    //Send Finish signal
    sync_server_client();

    FreePM(cceh_cache->dir);
    FreePM(cceh_cache);
}

int cceh_client ()
{
    uint64_t	num_threads = 1;
    std::vector<std::thread> client_threads;
    
    void	   *status;
    
    for (uint64_t i = 0; i < num_threads; i++) {
        client_threads.push_back(std::thread(client_thread_func,i));
    }

    for (uint64_t i = 0; i < num_threads; i++) {
        client_threads[i].join();
    }
    return 0;
}
