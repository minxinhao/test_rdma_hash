#include <math.h>

#include "RACE.h"
#include "debug.h"
#include "NVM.h"
#include "ib.h"
#include "setup_ib.h"
#include "config.h"

void Init(struct CCEH* cceh){
    cceh->global_depth = INIT_DEPTH;
    cceh->resize_lock = 0;
    uint64_t dir_size = pow(2,INIT_DEPTH);
    struct Segment* tmp;
    for(uint64_t i = 0 ; i < pow(2,INIT_DEPTH) ; i++){
        tmp=(struct Segment*)AllocPM(sizeof(struct Segment));
        cceh->dir[i].seg_ptr = (uintptr_t)tmp;
        cceh->dir[i].local_depth = INIT_DEPTH;
        for(uint64_t j = 0 ; j < BUCKET_PER_SEGMENT*3 ; j++){
            tmp->buckets[j].local_depth = INIT_DEPTH;
            tmp->buckets[j].suffix = i;
        }
    }
}

void Destroy(struct CCEH* cceh){
    struct Segment* tmp;
    uint64_t cur_depth,i=0;
    uint64_t stride, buddy ;
    uint64_t dir_size = pow(2,cceh->global_depth);
    log_message("Dir_siz:%lu",dir_size);
    if(cceh->resize_lock) log_message("Inproccessing Resize");
    // while(i < dir_size){
        // tmp=(struct Segment*)(cceh->dir[i].seg_ptr);
        // uint64_t cur_depth = tmp->buckets[0].local_depth;
        // stride = pow(2,cceh->global_depth - cur_depth);
        // log_message("stride:%lu",stride);
        // buddy = i + stride;
        // for(uint64_t j = i ; j < buddy ; j++ ){
        //     tmp = (struct Segment*)(cceh->dir[j].seg_ptr);
        //     if(tmp->buckets[0].local_depth != cur_depth) log_message("Inconsisten Depth in Segment:%lu",j)
        // }
        // i = buddy;
        // FreePM((char*)(cceh->dir[i].seg_ptr));
    // }
}


void PrintCCEH(struct CCEH* cceh){
    log_message("---------PrintCCEH-----");
    log_message("Global Depth:%lu",cceh->global_depth);
    uint64_t dir_size = pow(2,cceh->global_depth);
    struct Segment *cur_seg;
    for(uint64_t i = 0 ; i < dir_size ; i++){
        log_message("seg_ptr:%lx",cceh->dir[i].seg_ptr);
        cur_seg=(struct Segment*)cceh->dir[i].seg_ptr;
        log_message("Segment:local_depth:%u suffix:%x seg_ptr:%lx",cur_seg->buckets[0].local_depth,
                cur_seg->buckets[0].suffix,cceh->dir[i].seg_ptr);
    }
}

uint64_t Slot2U64(struct slot* slot){
    uint64_t tmp_fp,tmp_len,tmp_ptr,tmp;
    // log_message("Slot2U64");
    // log_message("Slot: fp:%x len:%x offset:%lx",slot->fp,slot->len,slot->offset);
    tmp_fp = slot->fp;
    tmp_len = slot->len;
    tmp_ptr = slot->offset;
    tmp = (tmp_fp << 56) | (tmp_len << 48) | tmp_ptr;
    return tmp;    
}

void U64Slot(uint64_t val,struct slot* slot){
    uint64_t tmp_fp,tmp_len,tmp_ptr,tmp;
    // log_message("U64Slot");
    slot->fp = val>>56;
    slot->len = val>>48 & ((1lu<<8)-1);
    slot->offset = val & ((1lu<<48)-1);
    // log_message("Slot: fp:%x len:%d offset:%lx",slot->fp,slot->len,slot->offset);
}

struct KVBlock* InitKVBlock(struct Slice* key, struct Slice* value){
    struct KVBlock* kv_block =(struct KVBlock*)AllocPM(2*sizeof(uint64_t)+key->len+value->len);
    kv_block->k_len = key->len;
    kv_block->v_len = value->len;
    memcpy(kv_block->data,key->data,key->len);
    memcpy(kv_block->data + key->len,value->data,value->len);
    return kv_block;
}

void DestoryKVBlock(struct KVBlock* kv_block){
    if(likely(kv_block!=NULL))
        FreePM((char*)kv_block);
}

