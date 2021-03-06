#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>

#include "debug.h"
#include "sock.h"

ssize_t sock_read (int sock_fd, void *buffer, size_t len)
{
    ssize_t nr, tot_read;
    char *buf = (char*)buffer; // avoid pointer arithmetic on void pointer                                    
    tot_read = 0;

    while (len !=0 && (nr = read(sock_fd, buf, len)) != 0) {
        if (nr < 0) {
            if (errno == EINTR) {
                continue;
            } else {
                return -1;
            }
        }
        len -= nr;
        buf += nr;
        tot_read += nr;
    }

    return tot_read;
}

ssize_t sock_write (int sock_fd, void *buffer, size_t len)
{
    ssize_t nw, tot_written;
    const char *buf = (char*)buffer;  // avoid pointer arithmetic on void pointer                             

    for (tot_written = 0; tot_written < len; ) {
        nw = write(sock_fd, buf, len-tot_written);

        if (nw <= 0) {
            if (nw == -1 && errno == EINTR) {
                continue;
            } else {
                return -1;
            }
        }

        tot_written += nw;
        buf += nw;
    }
    return tot_written;
}

int sock_create_bind (char* ip_addr,char *port)
{
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    int sock_fd = -1, ret = 0;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;
    hints.ai_flags = AI_PASSIVE;

    ret = getaddrinfo(ip_addr, port, &hints, &result);
    check(ret==0, "getaddrinfo error.");

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sock_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock_fd < 0) {
            continue;
        }

        ret = bind(sock_fd, rp->ai_addr, rp->ai_addrlen);
        if (ret == 0) {
            /* bind success */
            break;
        }

        close(sock_fd);
        sock_fd = -1;
    }

    check(rp != NULL, "creating socket.");

    freeaddrinfo(result);
    return sock_fd;

 error:
    if (result) {
        freeaddrinfo(result);
    }
    if (sock_fd > 0) {
        close(sock_fd);
    }
    return -1;
}

int sock_create_connect (char *ip_addr, char *port)
{
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    int sock_fd = -1, ret = 0;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;

    ret = getaddrinfo(ip_addr, port, &hints, &result);
    check(ret==0, "[ERROR] %s", gai_strerror(ret));

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sock_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock_fd == -1) {
            continue;
        }

        ret = connect(sock_fd, rp->ai_addr, rp->ai_addrlen);
        if (ret == 0) {
            /* connection success */
            break;
        }

        close(sock_fd);
        sock_fd = -1;
    }

    check(rp!=NULL, "could not connect: %s %s", ip_addr, port);

    freeaddrinfo(result);
    return sock_fd;

 error:
    if (result) {
        freeaddrinfo(result);
    }
    if (sock_fd != -1) {
        close(sock_fd);
    }
    return -1;
}

int sock_set(int sock_fd, char *ptr,size_t data_size)
{
    int n;
    n = sock_write(sock_fd, ptr, data_size);
    check(n==data_size, "write data to socket.");

    return 0;
 error:
    return -1;
}

int sock_get(int sock_fd, char *ptr,size_t data_size)
{
    int n;
    n = sock_read(sock_fd,ptr, data_size);
    check(n==data_size, "read data from socket.");

    return 0;
 error:
    return -1;
}

