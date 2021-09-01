#include "rpc.h"

#include <unordered_map>
#include <iostream>

namespace raft {

// ==================================================================================================
// [AE_RPC] [AE_RPC,leader_term,leader_id,prev_log_index,prev_log_term,leader_commit,logsize\r\n<logs>]
// <logs> <[items][log1][log2]...>
// [RV_RPC] [RV_RPC,candidate_term,candidate_id,last_log_index,last_log_term\r\n]
// [HB_RPC] [HB_RPC,leader_term,leader_id,prev_log_index,prev_log_term,leader_commit\r\n]
// [IS_RPC] [IS_RPC,leader_term,leader_id,last_included_index,last_included_term,offset,done,datasize\r\n<data>]
// [AE_REPLY] [AE_REPLY,term,success\r\n]
// [RV_REPLY] [RV_REPLY,term,success\r\n]
// ==================================================================================================

static int getrpctype(const std::string& type)
{
    static std::unordered_map<std::string, int> typemap = {
        { "AE_RPC", AE_RPC },
        { "RV_RPC", RV_RPC },
        { "HB_RPC", HB_RPC },
        { "IS_RPC", IS_RPC },
        { "AE_REPLY", AE_REPLY },
        { "RV_REPLY", RV_REPLY },
        { "IS_REPLY", IS_REPLY },
    };
    return typemap.count(type) ? typemap[type] : NONE;
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

void rpc::parse(angel::buffer& buf, int crlf)
{
    const char *s = buf.peek();
    const char *es = s + crlf;
    const char *p = std::find(s, es, ',');
    type = getrpctype(std::string(s, p));
    if (type == NONE) return;
    p += 1; // skip ','
    auto indexs = split(p, es, ',');
    std::string ts;
    switch (type) {
    case HB_RPC: {
        AppendEntry ae;
        ts.assign(p, p + indexs[0]);
        ae.leader_term = stoul(ts);
        ae.leader_id.assign(p + indexs[0] + 1, p + indexs[1]);
        ts.assign(p + indexs[1] + 1, p + indexs[2]);
        ae.prev_log_index = stoul(ts);
        ts.assign(p + indexs[2] + 1, p + indexs[3]);
        ae.prev_log_term = stoul(ts);
        ts.assign(p + indexs[3] + 1, es);
        ae.leader_commit = stoul(ts);
        buf.retrieve(crlf + 2);
        msg = ae;
        break;
    }
    case AE_RPC: {
        AppendEntry ae;
        // parse logsize，看RPC是否完整
        ts.assign(p + indexs[4] + 1, es);
        size_t logsize = stoul(ts);
        if (buf.readable() < crlf + 2 + logsize) {
            type = NONE;
            break;
        }
        ts.assign(p, p + indexs[0]);
        ae.leader_term = stoul(ts);
        ae.leader_id.assign(p + indexs[0] + 1, p + indexs[1]);
        ts.assign(p + indexs[1] + 1, p + indexs[2]);
        ae.prev_log_index = stoul(ts);
        ts.assign(p + indexs[2] + 1, p + indexs[3]);
        ae.prev_log_term = stoul(ts);
        ts.assign(p + indexs[3] + 1, p + indexs[4]);
        ae.leader_commit = stoul(ts);
        // items,[term1,len,cmd1][term2,len,cmd2]
        es += 2; // skip '\r\n'
        s = es;
        es += logsize;
        p = std::find(s, es, ',');
        size_t items = stoul(std::string(s, p));
        for ( ; items > 0; items--) {
            s = p + 1;
            p = std::find(s, es, ',');
            size_t term = stoul(std::string(s, p));
            s = p + 1;
            p = std::find(s, es, ',');
            size_t len = stoul(std::string(s, p));
            ae.logs.emplace_back(term, std::string(p + 1, len));
            p += len;
        }
        buf.retrieve(crlf + 2 + logsize);
        msg = ae;
        break;
    }
    case RV_RPC: {
        RequestVote rv;
        ts.assign(p, p + indexs[0]);
        rv.candidate_term = stoul(ts);
        rv.candidate_id.assign(p + indexs[0] + 1, p + indexs[1]);
        ts.assign(p + indexs[1] + 1, p + indexs[2]);
        rv.last_log_index = stoul(ts);
        ts.assign(p + indexs[2] + 1, es);
        rv.last_log_term = stoul(ts);
        buf.retrieve(crlf + 2);
        msg = rv;
        break;
    }
    case IS_RPC: {
        InstallSnapshot snapshot;
        ts.assign(p + indexs[5] + 1, es);
        size_t datasize = stoul(ts);
        if (buf.readable() < crlf + 2 + datasize) {
            type = NONE;
            break;
        }
        ts.assign(p, p + indexs[0]);
        snapshot.leader_term = stoul(ts);
        snapshot.leader_id.assign(p + indexs[0] + 1, p + indexs[1]);
        ts.assign(p + indexs[1] + 1, p + indexs[2]);
        snapshot.last_included_index = stoul(ts);
        ts.assign(p + indexs[2] + 1, p + indexs[3]);
        snapshot.last_included_term = stoul(ts);
        ts.assign(p + indexs[3] + 1, p + indexs[4]);
        snapshot.offset = stoul(ts);
        ts.assign(p + indexs[4] + 1, p + indexs[5]);
        snapshot.done = stoul(ts);
        snapshot.data.assign(es + 2, datasize);
        buf.retrieve(crlf + 2 + datasize);
        msg = snapshot;
        break;
    }
    case AE_REPLY: case RV_REPLY: case IS_REPLY: {
        Reply reply;
        ts.assign(p, p + indexs[0]);
        reply.term = stoul(ts);
        ts.assign(p + indexs[0] + 1, es);
        reply.success = stoi(ts);
        buf.retrieve(crlf + 2);
        msg = reply;
        break;
    }
    }
}

size_t rpc::getterm()
{
    switch (type) {
    case AE_RPC: case HB_RPC:
        return ae().leader_term;
    case RV_RPC:
        return rv().candidate_term;
    case IS_RPC:
        return snapshot().leader_term;
    case AE_REPLY: case RV_REPLY: case IS_REPLY:
        return reply().term;
    }
    return 0;
}

void rpc::print()
{
    switch (type) {
    case RV_RPC:
        printf("RV_RPC: { candidate_term: %zu, candidate_id: %s, "
                "last_log_index: %zu, last_log_term: %zu }\n",
                rv().candidate_term, rv().candidate_id.c_str(),
                rv().last_log_index, rv().last_log_term);
        break;
    case AE_RPC:
        printf("AE_RPC: { leader_term: %zu, leader_id: %s, "
                "prev_log_index: %zu, prev_log_term: %zu, leader_commit: %zu, ",
                ae().leader_term, ae().leader_id.c_str(), ae().prev_log_index,
                ae().prev_log_term, ae().leader_commit);
        printf("logs(");
        for (int i = 0; i < ae().logs.size(); i++) {
            printf("%zu", ae().logs[i].term);
            if (i < ae().logs.size() - 1)
                putchar(',');
        }
        printf(") }\n");
        break;
    case HB_RPC:
        printf("HB_RPC: { leader_term: %zu, leader_id: %s, "
                "prev_log_index: %zu, prev_log_term: %zu, leader_commit: %zu }\n",
                ae().leader_term, ae().leader_id.c_str(), ae().prev_log_index,
                ae().prev_log_term, ae().leader_commit);
        break;
    case RV_REPLY:
        printf("RV_REPLY: { term: %zu, voted: %s }\n",
                reply().term, reply().success ? "true" : "false");
        break;
    case AE_REPLY:
        printf("AE_REPLY: { term: %zu, success: %s }\n",
                reply().term, reply().success ? "true" : "false");
        break;
    }
}

}
