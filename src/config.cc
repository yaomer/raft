#include "config.h"

#include <assert.h>

#include <iostream>

#include <angel/logger.h>
#include <angel/util.h>

namespace raft {

struct rconf rconf;

void readConf(const std::string& confile)
{
    rconf.confile = confile;
    auto paramlist = angel::util::parse_conf(confile.c_str());
    for (auto& param : paramlist) {
        if (param[0].compare("self") == 0) {
            rconf.self.host = param[1] + ":" + param[2];
        } else if (param[0].compare("node") == 0) {
            node_info node;
            node.host = param[1] + ":" + param[2];
            rconf.nodes.emplace_back(node);
        } else if (param[0].compare("election_timeout") == 0) {
            rconf.election_timeout.base = stoi(param[1]);
            rconf.election_timeout.range = stoi(param[2]);
        } else if (param[0].compare("heartbeat_period") == 0) {
            rconf.heartbeat_period = stoi(param[1]);
        } else if (param[0].compare("snapshot_threshold") == 0) {
            rconf.snapshot_threshold = stoi(param[1]);
        } else if (param[0].compare("use_read_index") == 0) {
            rconf.use_read_index = stoi(param[1]);
        } else if (param[0].compare("use_lease_read") == 0) {
            rconf.use_lease_read = stoi(param[1]);
        } else if (param[0].compare("learner") == 0) {
            rconf.learner = stoi(param[1]);
        } else {
            log_fatal("unknown option<%s>", param[0].c_str());
        }
    }
    if (rconf.self.host.empty()) {
        log_fatal("You must have the <self> option");
    }
    if (!rconf.use_read_index && !rconf.use_lease_read) {
        log_fatal("You have to choose between ReadIndex and LeaseRead");
    }
}

}
