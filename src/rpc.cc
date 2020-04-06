#include "rpc.h"

#include <unordered_map>

using namespace raft;

// ==================================================================================================
// [ae_rpc] [ae_rpc,leaderTerm,leaderId,prevLogIndex,prevLogTerm,leaderCommit,logEntry(term,cmd)\r\n]
// [rv_rpc] [rv_rpc,candidateTerm,candidateId,lastLogIndex,lastLogTerm\r\n]
// [hb_rpc] [hb_rpc,leaderTerm,leaderId,prevLogIndex,prevLogTerm,leaderCommit\r\n]
// [ae_reply] [ae_reply,term,success\r\n]
// [rv_reply] [rv_reply,term,success\r\n]
// ==================================================================================================

static int getRpcType(const std::string& type)
{
    static std::unordered_map<std::string, int> typeMap = {
        { "ae_rpc", ae_rpc },
        { "rv_rpc", rv_rpc },
        { "hb_rpc", hb_rpc },
        { "ae_reply", ae_reply },
        { "rv_reply", rv_reply },
    };
    auto it = typeMap.find(type);
    if (it != typeMap.end()) return it->second;
    else return none;
}

// 返回所有c相对于s的偏移
static std::vector<size_t> split(const char *s, const char *es, char c)
{
    const char *p;
    const char *start = s;
    std::vector<size_t> indexs;
    while (true) {
        p = std::find(s, es, c);
        if (p == es) break;
        indexs.push_back(p - start);
        s = p + 1;
    }
    return indexs;
}

// for [ae_reply,1,1\r\n], s -> 'a', es -> '\r'
void raft::parserpc(rpc& r, const char *s, const char *es)
{
    const char *p = std::find(s, es, ',');
    std::string type(s, p);
    r.type = getRpcType(type);
    if (r.type == none) return;
    p += 1; // skip ','
    auto indexs = split(p, es, ',');
    std::string ts;
    switch (r.type) {
    case ae_rpc: case hb_rpc: {
        ts.assign(p, p+indexs[0]);
        r.msg.ae.leaderTerm = atoll(ts.c_str());
        r.msg.ae.leaderId.assign(p+indexs[0]+1, p+indexs[1]);
        ts.assign(p+indexs[1]+1, p+indexs[2]);
        r.msg.ae.prevLogIndex = atoll(ts.c_str());
        ts.assign(p+indexs[2]+1, p+indexs[3]);
        r.msg.ae.prevLogTerm = atoll(ts.c_str());
        if (r.type == hb_rpc) {
            ts.assign(p+indexs[3]+1, es);
            r.msg.ae.leaderCommit = atoll(ts.c_str());
            break;
        }
        ts.assign(p+indexs[3]+1, p+indexs[4]);
        r.msg.ae.leaderCommit = atoll(ts.c_str());
        ts.assign(p+indexs[4]+1, p+indexs[5]);
        r.msg.ae.LogEntry.term = atoll(ts.c_str());
        r.msg.ae.LogEntry.cmd.assign(p+indexs[5]+1, es);
        break;
    }
    case rv_rpc: {
        ts.assign(p, p+indexs[0]);
        r.msg.rv.candidateTerm = atoll(ts.c_str());
        r.msg.rv.candidateId.assign(p+indexs[0]+1,p+indexs[1]);
        ts.assign(p+indexs[1]+1, p+indexs[2]);
        r.msg.rv.lastLogIndex = atoll(ts.c_str());
        ts.assign(p+indexs[2]+1, es);
        r.msg.rv.lastLogTerm = atoll(ts.c_str());
        break;
    }
    case ae_reply: case rv_reply: {
        ts.assign(p, p+indexs[0]);
        r.msg.reply.term = atoll(ts.c_str());
        ts.assign(p+indexs[0]+1, es);
        r.msg.reply.success = atoi(ts.c_str());
        break;
    }
    case none:
        break;
    }
}

size_t raft::getterm(const rpc& r)
{
    switch (r.type) {
    case ae_rpc: case hb_rpc: return r.msg.ae.leaderTerm;
    case ae_reply: case rv_reply: return r.msg.reply.term;
    case rv_rpc: return r.msg.rv.candidateTerm;
    }
    return 0;
}

// format print rpc for debug
void raft::printrpc(const rpc& r)
{
    switch (r.type) {
    case rv_rpc:
        printf("{'type':'rv_rpc', 'candidateTerm':'%zu', 'candidateId':'%s', "
                "'lastLogIndex':'%zu', 'lastLogTerm':'%zu'}\n",
                r.msg.rv.candidateTerm, r.msg.rv.candidateId.c_str(),
                r.msg.rv.lastLogIndex, r.msg.rv.candidateTerm);
        break;
    case ae_rpc:
        printf("{'type':'ae_rpc', 'leaderTerm':'%zu', 'leaderId':'%s', "
                "'prevLogIndex':'%zu', 'prevLogTerm':'%zu', 'leaderCommit':'%zu', "
                "'[logTerm':'%zu', 'logCmd':'%s']}\n", r.msg.ae.leaderTerm,
                r.msg.ae.leaderId.c_str(), r.msg.ae.prevLogIndex, r.msg.ae.prevLogTerm,
                r.msg.ae.leaderCommit, r.msg.ae.LogEntry.term, r.msg.ae.LogEntry.cmd.c_str());
        break;
    case hb_rpc:
        printf("{'type':'hb_rpc', 'leaderTerm':'%zu', 'leaderId':'%s', "
                "'prevLogIndex':'%zu', 'prevLogTerm':'%zu', 'leaderCommit':'%zu'}\n",
                r.msg.ae.leaderTerm, r.msg.ae.leaderId.c_str(), r.msg.ae.prevLogIndex,
                r.msg.ae.prevLogTerm, r.msg.ae.leaderCommit);
        break;
    case rv_reply:
        printf("{'type':'rv_reply', 'term':'%zu', 'voted':'%s'}\n",
                r.msg.reply.term, r.msg.reply.success ? "true" : "false");
        break;
    case ae_reply:
        printf("{'type':'ae_reply', 'term':'%zu', 'success':'%s'}\n",
                r.msg.reply.term, r.msg.reply.success ? "true" : "false");
        break;
    }
}
