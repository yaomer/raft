#ifndef _RAFT_SRC_RPC_H
#define _RAFT_SRC_RPC_H

#include <string>
#include <vector>
#include <variant>

namespace raft {

struct LogEntry {
    LogEntry() = default;
    LogEntry(size_t leader_term, const std::string& command)
        : leader_term(leader_term), cmd(command)
    {
    }
    size_t leader_term; // 创建该条日志的领导人的任期
    std::string cmd;    // 要执行的命令
};

using LogEntryList = std::vector<LogEntry>;

enum RpcType {
    NONE,
    AE_RPC,
    RV_RPC,
    HB_RPC,
    AE_REPLY,
    RV_REPLY,
};

// 因为term永远大于0，所以我们用prev_log_term=0来处理以下特殊情况：
// 1. leader上现在还没有日志
// 2. leader上只有一条日志
// 所以当发现prev_log_term=0时，只需忽略prev_log_index和prev_log_term即可
// (之所以不设置为-1，因为都是size_t)
struct AppendEntry {
    AppendEntry() = default;
    size_t leader_term;     // 领导人的任期号
    std::string leader_id;  // 领导人的id
    size_t prev_log_index;  // 最新日志之前日志的索引
    size_t prev_log_term;   // 最新日志之前日志的任期号
    LogEntry log_entry;     // 要存储的日志条目
    size_t leader_commit;   // 领导人已提交的日志的索引
};

// last_log_term=0表明leader上现在还没有日志，忽略last_log_index和last_log_term即可
struct RequestVote {
    RequestVote() = default;
    size_t candidate_term;      // 候选人的任期号
    std::string candidate_id;   // 请求投票的候选人id
    size_t last_log_index;      // 候选人最新日志条目的索引
    size_t last_log_term;       // 候选人最新日志条目对应的任期号
};

// 用于回复AE RPC时，term用于领导人更新自己，一致性检查成功则success为真
// 用于回复RV RPC时，term用于候选人更新自己，将票投给了候选人则success为真
struct Reply {
    Reply() = default;
    size_t term;
    bool success;
};

class rpc {
public:
    using rpcmsg = std::variant<AppendEntry, RequestVote, Reply>;
    rpc() : type(NONE) {  }
    int gettype() { return type; }
    void parse(const char *s, const char *es);
    AppendEntry& ae() { return std::get<AppendEntry>(msg); }
    RequestVote& rv() { return std::get<RequestVote>(msg); }
    Reply& reply() { return std::get<Reply>(msg); }
    size_t getterm();
    void print();
private:
    int type;
    rpcmsg msg;
};

}

#endif // _RAFT_SRC_RPC_H
