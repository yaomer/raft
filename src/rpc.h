#ifndef _RAFT_SRC_RPC_H
#define _RAFT_SRC_RPC_H

#include "log_entry.h"

namespace raft {

enum rpctype {
    none,
    ae_rpc,
    rv_rpc,
    hb_rpc,
    ae_reply,
    rv_reply,
};

// 因为term永远大于0，所以我们用prevLogTerm=0来处理以下特殊情况：
// 1. leader上现在还没有日志
// 2. leader上只有一条日志
// 所以当发现prevLogTerm=0时，只需忽略prevLogIndex和prevLogTerm即可
// (之所以不设置为-1，因为都是size_t)
struct AppendEntry {
    AppendEntry() = default;
    size_t leaderTerm; // 领导人的任期号
    std::string leaderId; // 领导人的id
    size_t prevLogIndex; // 最新日志之前日志的索引
    size_t prevLogTerm; // 最新日志之前日志的任期号
    LogEntry LogEntry; // 要存储的日志条目
    size_t leaderCommit; // 领导人已提交的日志的索引
};

// lastLogTerm=0表明leader上现在还没有日志，忽略lastLogIndex和lastLogTerm即可
struct RequestVote {
    RequestVote() = default;
    size_t candidateTerm; // 候选人的任期号
    std::string candidateId; // 请求投票的候选人id
    size_t lastLogIndex; // 候选人最新日志条目的索引
    size_t lastLogTerm; // 候选人最新日志条目对应的任期号
};

// 用于回复AE RPC时，term用于领导人更新自己，一致性检查成功则success为真
// 用于回复RV RPC时，term用于候选人更新自己，将票投给了候选人则success为真
struct Reply {
    Reply() = default;
    size_t term;
    bool success;
};

struct rpc {
    rpc() = default;
    int type;
    union rpcmsg {
        rpcmsg() {  }
        ~rpcmsg() {  }
        AppendEntry ae;
        RequestVote rv;
        Reply reply;
    } msg;
};

void parserpc(rpc& r, const char *s, const char *es);
size_t getterm(const rpc& r);
void printrpc(const rpc& r);

}

#endif // _RAFT_SRC_RPC_H
