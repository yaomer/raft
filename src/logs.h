#ifndef _RAFT_SRC_LOGS_H
#define _RAFT_SRC_LOGS_H

#include "rpc.h"

namespace raft {

class Logs {
public:
    Logs() {  }
    ~Logs();
    void setLogFile(const std::string& file);
    void append(size_t term, const std::string& cmd);
    void remove(size_t from);
    void removeBefore(size_t to);
    void load();
    bool empty() { return end() == 0; }
    size_t baseIndex() { return base_index; }
    size_t lastTerm();
    void setLastLog(size_t last_index, size_t last_term);
    size_t size() { return logs.size(); }
    size_t end() { return base_index + logs.size(); }
    LogEntry& operator[](size_t i) { return logs[i - base_index]; }
private:
    std::vector<LogEntry> logs;
    size_t base_index = 0;
    size_t last_included_term = 0;
    std::string logfile;
    int logfd = -1;
};

}

#endif // _RAFT_SRC_LOGS_H
