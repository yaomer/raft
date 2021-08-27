#ifndef _RAFT_SRC_CONFIG_H

#include <vector>
#include <string>

namespace raft {

struct node_info {
    std::string ip;
    int port;
};

struct rconf {
    std::vector<node_info> nodes;
    size_t election_timeout = 3000;
    size_t heartbeat_period = 1000;
    size_t server_cron_period = 100;
    std::string confile = "../raft.conf";
    std::string logfile;
};

extern struct rconf rconf;

void readConf(const std::string& confile);

}

#endif // _RAFT_SRC_CONFIG_H
