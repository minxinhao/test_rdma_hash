#define _GNU_SOURCE
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
    char *buf = buffer; // avoid pointer arithmetic on void pointer                                    
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
    const char *buf = buffer;  // avoid pointer arithmetic on void pointer                             

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

int sock_set_qp_info(int sock_fd, struct QPInfo *qp_info)
{
    int n;
    struct QPInfo tmp_qp_info;

    tmp_qp_info.lid       = htons(qp_info->lid);
    tmp_qp_info.qp_num    = htonl(qp_info->qp_num);
    tmp_qp_info.interface_id    = htonl(qp_info->interface_id);
    tmp_qp_info.subnet_prefix    = htonl(qp_info->subnet_prefix);

    n = sock_write(sock_fd, (char *)&tmp_qp_info, sizeof(struct QPInfo));
    check(n==sizeof(struct QPInfo), "write qp_info to socket.");

    return 0;

 error:
    return -1;
}

int sock_get_qp_info(int sock_fd, struct QPInfo *qp_info)
{
    int n;
    struct QPInfo  tmp_qp_info;

    n = sock_read(sock_fd, (char *)&tmp_qp_info, sizeof(struct QPInfo));
    check(n==sizeof(struct QPInfo), "read qp_info from socket.");

    qp_info->lid       = ntohs(tmp_qp_info.lid);
    qp_info->qp_num    = ntohl(tmp_qp_info.qp_num);
    qp_info->interface_id    = ntohl(tmp_qp_info.interface_id);
    qp_info->subnet_prefix    = ntohl(tmp_qp_info.qp_num);

    return 0;

 error:
    return -1;
}


int sock_set_remote_addr(int sock_fd, struct RemoteAddr *remote_addr)
{
    int n;
    struct RemoteAddr tmp_remote_addr;

    tmp_remote_addr.remote_addr    = htobe64(remote_addr->remote_addr);
    tmp_remote_addr.rkey    = htonl(remote_addr->rkey);

    n = sock_write(sock_fd, (char *)&tmp_remote_addr, sizeof(struct RemoteAddr));
    check(n==sizeof(struct RemoteAddr), "write remote_addr to socket.");

    return 0;

 error:
    return -1;
}

int sock_get_remote_addr(int sock_fd, struct RemoteAddr *remote_addr)
{
    int n;
    struct RemoteAddr  tmp_remote_addr;

    n = sock_read(sock_fd, (char *)&tmp_remote_addr, sizeof(struct RemoteAddr));
    check(n==sizeof(struct RemoteAddr), "read remote_addr from socket.");

    remote_addr->remote_addr       = be64toh(tmp_remote_addr.remote_addr);
    remote_addr->rkey    = ntohl(tmp_remote_addr.rkey);

    return 0;

 error:
    return -1;
}
