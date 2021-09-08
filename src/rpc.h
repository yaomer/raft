#ifndef _RAFT_SRC_RPC_H
#define _RAFT_SRC_RPC_H

#include <string>
#include <vector>
#include <variant>

#include <angel/buffer.h>

namespace raft {

struct LogEntry {
    LogEntry() = default;
    LogEntry(size_t leader_term, std::string&& command)
        : term(leader_term), cmd(std::move(command))
    {
    }
    size_t term; // 创建该条日志的领导人的任期
    std::string cmd;    // 要执行的命令
};

enum RpcType {
    NONE,
    AE_RPC,
    RV_RPC,
    HB_RPC,
    IS_RPC,
    AE_REPLY,
    RV_REPLY,
    HB_REPLY,
    IS_REPLY,
};

// prev_log_term = 0表示这是leader上的第一条日志
struct AppendEntry {
    AppendEntry() = default;
    size_t leader_term;     // 领导人的任期号
    std::string leader_id;  // 领导人的id
    size_t prev_log_index;  // 最新日志之前日志的索引
    size_t prev_log_term;   // 最新日志之前日志的任期号
    std::vector<LogEntry> logs; // 要存储的日志条目
    size_t leader_commit;   // 领导人已提交的日志的索引
};

// last_log_term = 0表明leader上日志为空
struct RequestVote {
    RequestVote() = default;
    size_t candidate_term;      // 候选人的任期号
    std::string candidate_id;   // 请求投票的候选人id
    size_t last_log_index;      // 候选人最新日志条目的索引
    size_t last_log_term;       // 候选人最新日志条目对应的任期号
};

struct InstallSnapshot {
    InstallSnapshot() = default;
    size_t leader_term;
    std::string leader_id;
    size_t last_included_index;
    size_t last_included_term;
    size_t offset;
    char *chunk_data;
    size_t chunk_size;
    std::string data;
    bool done;
};

// 用于回复AE RPC时，term用于领导人更新自己，一致性检查成功则success为真
// 用于回复RV RPC时，term用于候选人更新自己，将票投给了候选人则success为真
// 用于回复IS RPC时，忽略success即可
struct Reply {
    Reply() = default;
    size_t term;
    bool success;
};

class rpc {
public:
    using rpcmsg = std::variant<AppendEntry, RequestVote, InstallSnapshot, Reply>;
    rpc() : type(NONE) {  }
    int gettype() { return type; }
    void parse(angel::buffer& buf, int crlf);
    AppendEntry& ae() { return std::get<AppendEntry>(msg); }
    RequestVote& rv() { return std::get<RequestVote>(msg); }
    InstallSnapshot& snapshot() { return std::get<InstallSnapshot>(msg); }
    Reply& reply() { return std::get<Reply>(msg); }
    size_t getterm();
    void print();
private:
    int type;
    rpcmsg msg;
};

struct RpcTypeString {
    static constexpr const char *inter = "<internal>";
    static constexpr const char *ae_rpc = "<internal>AE_RPC";
    static constexpr const char *rv_rpc = "<internal>RV_RPC";
    static constexpr const char *hb_rpc = "<internal>HB_RPC";
    static constexpr const char *is_rpc = "<internal>IS_RPC";
    static constexpr const char *ae_reply = "<internal>AE_REPLY";
    static constexpr const char *rv_reply = "<internal>RV_REPLY";
    static constexpr const char *hb_reply = "<internal>HB_REPLY";
    static constexpr const char *is_reply = "<internal>IS_REPLY";
};

extern struct RpcTypeString rpcts;

}

#endif // _RAFT_SRC_RPC_H
