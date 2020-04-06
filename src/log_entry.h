#ifndef _RAFT_SRC_LOG_ENTRY_H
#define _RAFT_SRC_LOG_ENTRY_H

#include <string>
#include <vector>

namespace raft {

struct LogEntry {
    LogEntry() = default;
    LogEntry(size_t leaderTerm, const std::string& command)
        : term(leaderTerm), cmd(command)
    {
    }
    size_t term; // 创建该条日志的领导人的任期
    std::string cmd; // 要执行的命令
};

using LogEntryList = std::vector<LogEntry>;

}

#endif // _RAFT_SRC_LOG_ENTRY_H
