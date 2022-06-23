#pragma once

#include <stdio.h>
#include <stdint.h>

// 10GB
#define PM_SIZE ((1llu<<30)*10) 
char* get_pmem_buf(uint64_t buf_size);

void free_pmem_buf(char* buf);

char *AllocPM(uint64_t size);

void FreePM(void* ptr);
