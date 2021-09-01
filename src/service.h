#ifndef _RAFT_SRC_SERVICE_H
#define _RAFT_SRC_SERVICE_H

#include <string>

namespace raft {

// 上层状态机(kv)应继承Service，并实现相应接口
struct Service {
    Service() : name("raft-state-machine") {  }
    virtual ~Service() {  }
    virtual void apply(const std::string& cmd) {  }
    virtual std::string reply() { return name;  }
    // 以追加的方式打开文件，将状态机数据追加到文件末尾
    virtual void saveSnapshot(const std::string& filename) {  }
    // 从offset处载入快照
    virtual void loadSnapshot(const std::string& filename, off_t offset) {  }
    // 快照是否创建完成，一般都会使用COW技术来优化
    virtual bool isSavedSnapshot() { return false; }

    std::string name;
};

}

#endif // _RAFT_SRC_SERVICE_H
