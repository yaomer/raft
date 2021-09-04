#ifndef _RAFT_SRC_CONFIG_H
#define _RAFT_SRC_CONFIG_H

#include <vector>
#include <string>

namespace raft {

struct node_info {
    // [ip:port]
    std::string host;
};

struct election_timeout_info {
    // election_timeout = base + rand() % range;
    int base = 150;
    int range = 150;
};

struct rconf {
    node_info self;
    std::vector<node_info> nodes;
    election_timeout_info election_timeout;
    int heartbeat_period = 100;
    int server_cron_period = 100;
    int snapshot_threshold = 100;
    bool learner = false;
    std::string confile;
    std::string statefile;
    std::string snapshot;
};

extern struct rconf rconf;

void readConf(const std::string& confile);

}

#endif // _RAFT_SRC_CONFIG_H
