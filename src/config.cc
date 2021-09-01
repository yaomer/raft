#include "config.h"

#include <assert.h>
#include <iostream>

namespace raft {

struct rconf rconf;

typedef std::vector<std::string> Param;
typedef std::vector<Param> Paramlist;

static Paramlist parseConf(const char *confile)
{
    char buf[1024];
    FILE *fp = fopen(confile, "r");
    if (!fp) {
        printf("can't open %s\n", confile);
        abort();
    }
    Paramlist paramlist;
    while (fgets(buf, sizeof(buf), fp)) {
        const char *s = buf;
        const char *es = buf + strlen(buf);
        Param param;
        do {
            s = std::find_if_not(s, es, isspace);
            if (s == es || s[0] == '#') break;
            const char *p = std::find_if(s, es, isspace);
            assert(p != es);
            param.emplace_back(s, p);
            s = p + 1;
        } while (true);
        if (!param.empty())
            paramlist.emplace_back(param);
    }
    fclose(fp);
    return paramlist;
}

void readConf(const std::string& confile)
{
    auto paramlist = parseConf(confile.c_str());
    for (auto& param : paramlist) {
        if (param[0].compare("node") == 0) {
            node_info node;
            node.ip = param[1];
            node.port = stoi(param[2]);
            rconf.nodes.emplace_back(node);
        } else if (param[0].compare("election_timeout") == 0) {
            rconf.election_timeout.base = stoi(param[1]);
            rconf.election_timeout.range = stoi(param[2]);
        } else if (param[0].compare("heartbeat_period") == 0) {
            rconf.heartbeat_period = stoi(param[1]);
        } else if (param[0].compare("server_cron_period") == 0) {
            rconf.server_cron_period = stoi(param[1]);
        } else if (param[0].compare("snapshot_threshold") == 0) {
            rconf.snapshot_threshold = stoi(param[1]);
        } else {
            printf("error: option<%s> unknown\n", param[0].c_str());
            abort();
        }
    }
}

}
