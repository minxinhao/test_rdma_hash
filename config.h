#ifndef CONFIG_H_
#define CONFIG_H_

#include <stdbool.h>
#include <inttypes.h>
#include <stdint.h>

struct ConfigInfo {    
    bool is_server;          /* if the current node is server */
    char *ip_address;         /* ip of server */
    char *sock_port;         /* socket port number */
    bool roce_flag;         /* type of underlying net */
    bool is_pm;         /* type of underlying net */
    char* pmem_path; // path of pmem file to be mapped into buf

    //sock file description used for tcp connect
    int sockfd	;
    int peer_sockfd;
}__attribute__((aligned(64)));

extern struct ConfigInfo config_info;

void print_config_info ();

#endif /* CONFIG_H_*/
