#include "rpc.h"

#include <unordered_map>
#include <iostream>

namespace raft {

// ==================================================================================================
// [AE_RPC] [AE_RPC,leader_term,leader_id,prev_log_index,prev_log_term,leader_commit,log_entry(term,cmd)\r\n]
// [RV_RPC] [RV_RPC,candidate_term,candidate_id,last_log_index,last_log_term\r\n]
// [HB_RPC] [HB_RPC,leader_term,leader_id,prev_log_index,prev_log_term,leader_commit\r\n]
// [AE_REPLY] [AE_REPLY,term,success\r\n]
// [RV_REPLY] [RV_REPLY,term,success\r\n]
// ==================================================================================================

static int getrpctype(const std::string& type)
{
    static std::unordered_map<std::string, int> typemap = {
        { "AE_RPC", AE_RPC },
        { "RV_RPC", RV_RPC },
        { "HB_RPC", HB_RPC },
        { "AE_REPLY", AE_REPLY },
        { "RV_REPLY", RV_REPLY },
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

// for [AE_REPLY,1,1\r\n], s -> 'a', es -> '\r'
void rpc::parse(const char *s, const char *es)
{
    const char *p = std::find(s, es, ',');
    type = getrpctype(std::string(s, p));
    if (type == NONE) return;
    p += 1; // skip ','
    auto indexs = split(p, es, ',');
    std::string ts;
    switch (type) {
    case AE_RPC: case HB_RPC: {
        AppendEntry ae;
        ts.assign(p, p + indexs[0]);
        ae.leader_term = stoul(ts);
        ae.leader_id.assign(p + indexs[0] + 1, p + indexs[1]);
        ts.assign(p + indexs[1] + 1, p + indexs[2]);
        ae.prev_log_index = stoul(ts);
        ts.assign(p + indexs[2] + 1, p + indexs[3]);
        ae.prev_log_term = stoul(ts);
        if (type == HB_RPC) {
            ts.assign(p + indexs[3] + 1, es);
            ae.leader_commit = stoul(ts);
            msg = ae;
            break;
        }
        ts.assign(p + indexs[3] + 1, p + indexs[4]);
        ae.leader_commit = stoul(ts);
        ts.assign(p + indexs[4] + 1, p + indexs[5]);
        ae.log_entry.leader_term = stoul(ts);
        ae.log_entry.cmd.assign(p + indexs[5] + 1, es);
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
        msg = rv;
        break;
    }
    case AE_REPLY: case RV_REPLY: {
        Reply reply;
        ts.assign(p, p + indexs[0]);
        reply.term = stoul(ts);
        ts.assign(p + indexs[0] + 1, es);
        reply.success = stoi(ts);
        msg = reply;
        break;
    }
    case NONE:
        break;
    }
}

size_t rpc::getterm()
{
    switch (type) {
    case AE_RPC: case HB_RPC:
        return ae().leader_term;
    case RV_RPC:
        return rv().candidate_term;
    case AE_REPLY: case RV_REPLY:
        return reply().term;
    }
    return 0;
}

// format print rpc for debug
void rpc::print()
{
    switch (type) {
    case RV_RPC:
        printf("{'type':'RV_RPC', 'candidate_term':'%zu', 'candidate_id':'%s', "
                "'last_log_index':'%zu', 'last_log_term':'%zu'}\n",
                rv().candidate_term, rv().candidate_id.c_str(),
                rv().last_log_index, rv().last_log_term);
        break;
    case AE_RPC:
        printf("{'type':'AE_RPC', 'leader_term':'%zu', 'leader_id':'%s', "
                "'prev_log_index':'%zu', 'prev_log_term':'%zu', 'leader_commit':'%zu', "
                "'log[term':'%zu', 'cmd':'%s']}\n", ae().leader_term,
                ae().leader_id.c_str(), ae().prev_log_index, ae().prev_log_term,
                ae().leader_commit, ae().log_entry.leader_term, ae().log_entry.cmd.c_str());
        break;
    case HB_RPC:
        printf("{'type':'HB_RPC', 'leader_term':'%zu', 'leader_id':'%s', "
                "'prev_log_index':'%zu', 'prev_log_term':'%zu', 'leader_commit':'%zu'}\n",
                ae().leader_term, ae().leader_id.c_str(), ae().prev_log_index,
                ae().prev_log_term, ae().leader_commit);
        break;
    case RV_REPLY:
        printf("{'type':'RV_REPLY', 'term':'%zu', 'voted':'%s'}\n",
                reply().term, reply().success ? "true" : "false");
        break;
    case AE_REPLY:
        printf("{'type':'AE_REPLY', 'term':'%zu', 'success':'%s'}\n",
                reply().term, reply().success ? "true" : "false");
        break;
    }
}

}
