#ifndef _RAFT_SRC_SERVICE_H
#define _RAFT_SRC_SERVICE_H

#include <string>

// 上层状态机(kv)应继承Service，并实现相应接口
struct Service {
    Service() { name = "raft-state-machine"; }
    virtual ~Service() {  }
    virtual void apply(const std::string& cmd) {  }
    virtual std::string reply() { return name;  }

    std::string name;
};

#endif // _RAFT_SRC_SERVICE_H
