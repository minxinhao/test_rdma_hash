#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include "NVM.h"
#include "debug.h"
#include "config.h"

#define MIN_SIZE (2ul*(1ul<<20))
int buf_fd;
size_t pmem_size;

char* get_pmem_buf(uint64_t buf_size){
    // buf_fd = open("/dev/dax0.0", O_RDWR);
    buf_fd = open(config_info.pmem_path, O_RDWR);
    if(buf_fd < 0)log_err("devdax open failed");
    
    pmem_size = (buf_size<MIN_SIZE)?MIN_SIZE:buf_size;  // Smaller sizes may fail
    void* buf = mmap(NULL, pmem_size, PROT_READ | PROT_WRITE, MAP_SHARED, buf_fd, 0);
    if(buf== MAP_FAILED)log_err("mmap failed for devdax");
    if(((uint64_t)buf)%256  != 0)log_err("Unaligned mapped address");
    memset(buf, 0, pmem_size);

    return (char*)buf;
}

void free_pmem_buf(char* buf){
    munmap(buf, pmem_size);//解除隐射才能释放cache空间
	close(buf_fd);
}