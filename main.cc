#include <stdio.h>

#include "debug.h"
#include "config.h"
#include "ib.h"
#include "setup_ib.h"
#include "client.h"
#include "server.h"

int main (int argc, char *argv[])
{
    int	ret = 0;

    if (argc != 5) {
        printf ("Usage: %s Type:[server/client] ip_addre sock_port pm_path\n", argv[0]);
        return 0;
    }    

    config_info.is_server = !strcmp(argv[1],"server");
    config_info.ip_address = argv[2];
    config_info.sock_port = argv[3];
    config_info.pmem_path = argv[4];
    config_info.roce_flag = 1;
    config_info.is_pm = true;

    print_config_info();
    
    ret = setup_ib ();
    check (ret == 0, "Failed to setup IB");

    if (config_info.is_server) {
        ret = cceh_server ();
    } else {
        ret = cceh_client ();
    }
    check (ret == 0, "Failed to run workload");

 error: 
    close_ib_connection ();
    return ret;
}    
