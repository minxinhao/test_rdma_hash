#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/utsname.h>

#include "debug.h"
#include "config.h"

struct ConfigInfo config_info;


void print_config_info ()
{
    log_message ("Configuraion");

    if (config_info.is_server) {
	log_message ("is_server                 = %s", "true");
    } else {
	log_message ("is_server                 = %s", "false");
    }
    log_message ("sock_port                 = %s", config_info.sock_port);
    log_message ("roce_flag                 = %d", config_info.roce_flag);

    log_message ("End of Configuraion");
}
