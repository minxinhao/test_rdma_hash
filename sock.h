#ifndef SOCK_H_
#define SOCK_H_

#include <inttypes.h>

#include "ib.h"

#define SOCK_SYNC_MSG     "sync"

ssize_t sock_read (int sock_fd, void *buffer, size_t len);
ssize_t sock_write (int sock_fd, void *buffer, size_t len);

int sock_create_bind (char *ip_addr,char *port);
int sock_create_connect (char *ip_addr, char *port);

int sock_set_qp_info(int sock_fd, struct QPInfo *qp_info);
int sock_get_qp_info(int sock_fd, struct QPInfo *qp_info);

int sock_set_remote_addr(int sock_fd, struct RemoteAddr *remote_addr);
int sock_get_remote_addr(int sock_fd, struct RemoteAddr *remote_addr);



#endif /* SOCK_H_ */
