#include <stdio.h>
#include <stdint.h>

char* get_pmem_buf(uint64_t buf_size);

void free_pmem_buf(char* buf);