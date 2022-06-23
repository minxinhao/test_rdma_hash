#pragma once
#include "hash.h"
struct Bucket;

// Global Depth 20应该够了吧
#define MAX_DEPTH 20
#define DIR_SIZE (1<<MAX_DEPTH) 
#define SLOT_PER_BUCKET (4)
#define BUCKET_BITS (2)
#define BUCKET_PER_SEGMENT (1<<2)
#define INIT_DEPTH (4)
#define BUCKET_MASK ((BUCKET_PER_SEGMENT*2)-1)

// #define SEGMENT_LOC(x,globale_depth) (x>>(8*sizeof(uint64_t)-globale_depth))
//use LSB as Segment Pattern
#define SEGMENT_LOC(x,globale_depth) (x&((1<<globale_depth)-1))
// #define BUCKET_LOC(x) (x&BUCKET_MASK)
#define BUCKET_LOC(x) (x>>(8*sizeof(uint64_t)-BUCKET_BITS-1))
#define TRUE_BUC_IDX(idx) ((idx/2)*3+(idx%2)*2)
#define TRUE_OVER_IDX(idx) ((idx/2)*3+(idx%2))
//连续读取over-flow和main-bucket两个数据
#define BUCKET_OFF(idx) (((idx/2)*3+(idx%2))*sizeof(struct Bucket))
#define MAIN_BUCKET(buc_idx,buc) ((buc_idx%2==0)? (buc):(buc+1))
#define MAIN_BUCKET_PTR(buc_idx,buc_ptr) ((buc_idx%2==0)? (buc_ptr):(buc_ptr+sizeof(struct Bucket)))
#define OVER_BUCKET(buc_idx,buc) ((buc_idx%2==0)? (buc+1):(buc))
#define OVER_BUCKET_PTR(buc_idx,buc_ptr) ((buc_idx%2==0)? (buc_ptr+sizeof(struct Bucket)):(buc_ptr))

//取24-32的8位hash值作为FP
#define FP(h) ((uint64_t)(h>>32)&((1<<8)-1))
//检查suffix和seg_loc前/后n位是否相同。其中suffix可能超出seg_loc，但是只要seg_loc后n位出一致即可。
#define CHECK_SUFFIX(suffix, seg_loc, global_depth) ((suffix&(1<<global_depth -1))^seg_loc)

//Define converation from uint64 to/from slot bit-field
#define SLOT_FP(val) (val>>56)
#define SLOT_LEN(val) (val>>48 & ((1lu<<8)-1))
#define SLOT_OFF(val) (val & ((1lu<<48)-1))


#define GLOBAL_BIT(v) ((v)>>((8*sizeof(uint64_t)-1)))
#define GLOBAL_BIT_MASK (1llu<<(8*sizeof(uint64_t)-1))

struct slot{
    uint8_t fp:8;
    uint8_t len:8;
    uint64_t offset:48;
}__attribute__((aligned(8)));

struct Bucket{
    uint32_t local_depth;
    uint32_t suffix;
    uint64_t slots[SLOT_PER_BUCKET];
}__attribute__((aligned(8)));

struct Segment{
    struct Bucket buckets[BUCKET_PER_SEGMENT*3];
}__attribute__((aligned(8)));

struct DirEntry{
    uint64_t local_split_lock;
    uintptr_t seg_ptr;
    uint64_t local_depth;
}__attribute__((aligned(8)));

struct CCEH{
    uint64_t resize_lock; 
    uint64_t global_depth;
    struct DirEntry dir[DIR_SIZE]; // Directory use MSB and is allocated enough space in advance.
}__attribute__((aligned(8)));

struct Slice{
    uint64_t len;
    char* data;
};

struct KVBlock{
    uint64_t k_len;
    uint64_t v_len;
    char data[0]; //变长数组，用来保证KVBlock空间上的连续性，便于RDMA操作
};

void Init(struct CCEH* cceh);
void Destroy(struct CCEH* cceh);

void PrintCCEH(struct CCEH* cceh);

uint64_t Slot2U64(struct slot* slot);
void U64Slot(uint64_t val,struct slot* slot);

struct KVBlock* InitKVBlock(struct Slice* key, struct Slice* value);
void DestoryKVBlock(struct KVBlock* kv_block);