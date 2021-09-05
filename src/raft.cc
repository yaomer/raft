#include "raft.h"
#include "util.h"

#include <iostream>
#include <random>
#include <algorithm>

#include <time.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <assert.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdarg.h>

using namespace raft;

struct LogType {
    // 普通用户命令日志
    static const unsigned char cmd = '$';
    // no-op日志
    static const unsigned char noop = '@';
    // 配置变更日志
    // Cold,new
    static const unsigned char old_new_config = 't';
    // Cnew
    static const unsigned char new_config = 'n';
    // Cold
    static const unsigned char old_config = 'o';
} logtype;

struct HeartBeatType {
    static const int normal = 0;
    static const int read_index = 1;
} hbtype;

void ServerNode::initServer()
{
    run_id = generateRunid();
    info("my runid is %s", run_id.c_str());
    connectNodes();
    setPaths();
    loadState();
    logs.load();
    loadSnapshot();
    int period = std::min(50, rconf.heartbeat_period);
    loop->run_every(period, [this]{ this->serverCron(); });
}

void ServerNode::connectNodes()
{
    for (auto& node : rconf.nodes) {
        if (node.host == self_host)
            continue;
        if (server_entries.count(node.host)) {
            log_fatal("The cluster contains duplicate nodes");
        }
        auto se = new ServerEntry(loop, angel::inet_addr(node.host));
        se->start(this);
        server_entries.emplace(node.host, se);
    }
}

void ServerNode::setPaths()
{
    mkdir("rlog", 0777);
    static char pid[64] = { 0 };
    snprintf(pid, sizeof(pid), "rlog/%s.log", run_id.c_str());
    logs.setLogFile(pid);
    snprintf(pid, sizeof(pid), "rlog/%s.state", run_id.c_str());
    rconf.statefile = pid;
    snprintf(pid, sizeof(pid), "rlog/%s.snapshot", run_id.c_str());
    rconf.snapshot = pid;
}

// 要如何保证系统的线性一致性呢？
// (所谓线性一致，简单理解，就是在t1时刻写入一个值，那么在t2时刻一定能读到它)
//
// 所有请求走一趟raft log，显然能够保证。
// 但读请求不会改变状态机的状态，显然是不需要记录日志的。
// 所以如果对每一个读请求都走一趟log，会显得很低效。
//
// 对于读请求，如果能够确定当前leader一定还是leader(没有被更大的term废除)
// 那么我们就可以直接从当前leader读取数据，而不必再走一趟log(raft保证leader一定拥有最新数据)
//
// 所以对于如何确认自己当前还是leader，raft paper中提供了两种方法
// 1. ReadIndex Read
// 1) 记录自己当前的commit index
// 2) 与其他节点交换一次心跳，如果能收到大多数节点的响应，就说明自己还是leader
// 3) 等待apply index超过commit index
// 4) 执行对应的读请求
// *) 这里有一个corner case需要注意，在leader当选后提交的no-op日志还没有提交前，
// 是不能提供ReadIndex的。因为no-op提交后才能保证leader拥有已提交的所有日志，
// 即保证commit index是最新的。

void ServerNode::process(const angel::connection_ptr& conn, angel::buffer& buf)
{
    rpc r;
    while (buf.readable() > 2) {
        int crlf = buf.find_crlf();
        if (crlf < 0) break;
        if (buf.starts_with("<user>")) { // 客户端的请求
            if (role == LEADER) {
                // <user><r>cmd\r\n <user>r get k
                // <user><w>cmd\r\n <user>w set k v
                unsigned char type = *(buf.peek() + 6);
                std::string cmd(buf.peek() + 7, crlf - 7);
                if (!cmd.empty()) {
                    if (type == 'r') { // 读请求
                        startReadIndex(conn->id(), cmd);
                    } else {
                        cmd.append(1, logtype.cmd);
                        logs.append(current_term, cmd);
                        info("append a new log[%zu](%zu)", logs.end() - 1, current_term);
                        clients.emplace(logs.end() - 1, conn->id());
                        sendLogEntry();
                    }
                }
            } else { // 重定向
                redirectToLeader(conn);
            }
            buf.retrieve(crlf + 2);
        } else if (buf.starts_with("<config>")) { // 集群配置变更
            if (role == LEADER) {
                std::string config(buf.peek() + 8, crlf - 8);
                startConfigChange(config);
            } else {
                redirectToLeader(conn);
            }
            buf.retrieve(crlf + 2);
        } else if (buf.starts_with(rpcts.inter)){ // 内部通信
            r.parse(buf, crlf);
            if (r.gettype() == NONE) break;
            processrpc(conn, r);
        } else {
            conn->send("Please input <user>msg\\r\\n\n");
            buf.retrieve(crlf + 2);
        }
    }
}

void ServerNode::startConfigChange(std::string& config)
{
    if (config.empty()) return;
    if (config_change_state) return;
    parseNodes(new_config_nodes, config.begin(), config.end());
    info("recvd new config <%s>", config.c_str());
    info("start member change");
    // 复制一份旧配置节点
    // 为了方便判断Cold,new日志是否在新旧配置中都达到了多数派
    std::string old_new_log;
    old_new_log.append(self_host).append(",");
    old_config_nodes.emplace(self_host);
    for (const auto& [host, serv] : server_entries) {
        old_config_nodes.emplace(host);
        old_new_log.append(host).append(",");
    }
    // 提交一条Cold,new日志
    old_new_log.back() = ';';
    old_new_log.append(config);
    old_new_log.append(1, logtype.old_new_config);
    logs.append(current_term, old_new_log);
    info("append a Cold,new log[%zu](%zu) <%s>", logs.end() - 1, current_term, old_new_log.c_str());
    config_change_state = OLD_NEW_CONFIG;
    // 同步到使用新旧配置的所有节点上
    applyOldNewConfig();
    sendLogEntry();
    setConfigChangeTimer();
}

void ServerNode::applyOldNewConfig()
{
    std::vector<std::string> old_new_nodes;
    std::vector<std::string> old_nodes(old_config_nodes.begin(), old_config_nodes.end());
    std::vector<std::string> new_nodes(new_config_nodes.begin(), new_config_nodes.end());
    std::sort(old_nodes.begin(), old_nodes.end());
    std::sort(new_nodes.begin(), new_nodes.end());
    std::set_union(old_nodes.begin(), old_nodes.end(), new_nodes.begin(), new_nodes.end(),
            std::back_inserter(old_new_nodes));
    for (const auto& host : old_new_nodes) {
        if (host != self_host && !server_entries.count(host)) {
            auto se = new ServerEntry(loop, angel::inet_addr(host));
            se->start(this);
            server_entries.emplace(host, se);
        }
    }
}

void ServerNode::setConfigChangeTimer()
{
    int timeout = getElectionTimeout();
    config_change_timer_id = loop->run_after(timeout, [this]{ this->rollbackConfigChange(); });
}

void ServerNode::cancelConfigChangeTimer()
{
    loop->cancel_timer(config_change_timer_id);
}

void ServerNode::rollbackConfigChange()
{
    std::string old_config(1, logtype.old_config);
    logs.append(current_term, old_config);
    sendLogEntry();
    if (!old_config_nodes.count(self_host)) {
        info("I'm not in the old configuration, ready to quit...");
        server_entries.clear();
        loop->quit();
    }
    rollbackOldConfig();
    clearConfigChangeInfo();
}

void ServerNode::rollbackOldConfig()
{
    // Cold,new -> Cold
    // 移除不在旧配置中的节点
    for (auto it = server_entries.begin(); it != server_entries.end(); ) {
        if (!old_config_nodes.count(it->first)) {
            it = server_entries.erase(it);
        } else {
            ++it;
        }
    }
    // Cnew -> Cold
    // 添加旧配置中有，但server_entries中没有的节点
    for (auto& host : old_config_nodes) {
        if (host != self_host && !server_entries.count(host)) {
            auto se = new ServerEntry(loop, angel::inet_addr(host));
            se->start(this);
            server_entries.emplace(host, se);
        }
    }
    updateConfigFile(old_config_nodes);
    info("cluster member change failed, rollback to the old configuration");
}

void ServerNode::startReadIndex(size_t id, const std::string& cmd)
{
    read_map[commit_index].emplace_back(id, cmd);
    apply_read_index = false;
    read_index_heartbeats++;
    sendHeartBeat(hbtype.read_index);
}

void ServerNode::sendLogEntry()
{
    for (auto& serv : server_entries) {
        if (serv.second->client->is_connected()) {
            sendLogEntry(serv.second.get());
        }
    }
}

void ServerNode::sendLogEntry(ServerEntry *serv)
{
    std::string buffer;
    auto& conn = serv->client->conn();
    if (serv->next_index < logs.baseIndex()) {
        sendSnapshot(conn);
        serv->next_index = logs.baseIndex();
    }
    size_t items = logs.end() - serv->next_index;
    if (items == 0) return;
    buffer.append(std::to_string(items)).append(",");
    for (size_t idx = serv->next_index; idx < logs.end(); idx++) {
        auto& next_log = logs[idx];
        buffer.append(std::to_string(next_log.term)).append(",");
        buffer.append(std::to_string(next_log.cmd.size())).append(",");
        buffer.append(next_log.cmd);
    }
    size_t prev_log_index = 0, prev_log_term = 0;
    if (serv->next_index > 0) {
        prev_log_index = serv->next_index - 1;
        if (prev_log_index < logs.baseIndex()) prev_log_term = last_included_term;
        else prev_log_term = logs[prev_log_index].term;
    }
    conn->format_send("%s,%zu,%s,%zu,%zu,%zu,%zu\r\n", rpcts.ae_rpc, current_term,
            run_id.c_str(), prev_log_index, prev_log_term, commit_index, buffer.size());
    conn->send(buffer);
}

void ServerNode::processRpcFromServer(const angel::connection_ptr& conn,
                                      angel::buffer& buf)
{
    rpc r;
    while (buf.readable() > 2) {
        int crlf = buf.find_crlf();
        if (crlf >= 0) {
            r.parse(buf, crlf);
            if (r.gettype() == NONE) break;
            processrpc(conn, r);
        } else
            break;
    }
}

void ServerNode::processrpc(const angel::connection_ptr& conn, rpc& r)
{
    auto term = r.getterm();
    if (term < current_term) {
        if (r.gettype() == IS_RPC) {
            sendReply(conn, IS_REPLY, true);
        }
        return;
    }
    if (term > current_term) {
        current_term = term;
        switch (role) {
        case LEADER: clearLeaderInfo(); break;
        case CANDIDATE: clearCandidateInfo(); break;
        case FOLLOWER: clearFollowerInfo(); break;
        }
        role = FOLLOWER;
    }
    switch (role) {
    case LEADER: processRpcAsLeader(conn, r); break;
    case CANDIDATE: processRpcAsCandidate(conn, r); break;
    case FOLLOWER: processRpcAsFollower(conn, r); break;
    }
    applyLogEntry();
}

void ServerNode::applyLogEntry()
{
    while (last_applied < commit_index) {
        // 此时[last_applied, commit_index)之间的所有日志条目都可以被应用到状态机
        auto& apply_log = logs[last_applied];
        if (apply_log.cmd.back() == logtype.cmd) {
            apply_log.cmd.pop_back();
            service->apply(apply_log.cmd);
            apply_log.cmd.append(1, logtype.cmd);
        }
        if (role == FOLLOWER && config_change_state == NEW_CONFIG) {
            if (apply_log.cmd.back() == logtype.new_config) {
                completeConfigChange();
            }
        }
        info("log[%zu](%zu) is applied to the state machine", last_applied, apply_log.term);
        if (role == LEADER) { // 回复客户端
            auto it = clients.find(last_applied);
            if (it != clients.end()) {
                auto conn = server->get_connection(it->second);
                if (conn && conn->is_connected())
                    conn->send(service->reply());
                clients.erase(it);
            }
        }
        ++last_applied;
    }
    applyReadIndex();
}

void ServerNode::applyReadIndex()
{
    // 未收到大多数节点的心跳响应
    if (!apply_read_index) return;
    // 节点还没有数据，正常情况不会出现
    if (commit_index == 0 && logs.empty()) {
        auto it = read_map.find(0);
        if (it != read_map.end()) {
            for (auto& [id, cmd] : it->second) {
                applyReadIndex(id, cmd);
            }
        }
        return;
    }
    if (commit_index == 0) return;
    auto& commit_log = logs[commit_index - 1];
    // no-op log还没有提交
    if (commit_log.term != current_term) return;
    while (!read_map.empty()) {
        auto& [read_index, reqlist] = *read_map.begin();
        if (read_index > last_applied) break;
        for (auto& [id, cmd] : reqlist) {
            applyReadIndex(id, cmd);
        }
        read_map.erase(read_index);
    }
}

void ServerNode::applyReadIndex(size_t id, const std::string& cmd)
{
    auto conn = server->get_connection(id);
    if (conn->is_connected()) {
        service->apply(cmd);
        conn->send(service->reply());
    }
}

void ServerNode::processRpcAsLeader(const angel::connection_ptr& conn, rpc& r)
{
    switch (r.gettype()) {
    case AE_REPLY:
        if (r.reply().success) sendLogEntrySuccessfully(conn);
        else sendLogEntryFail(conn);
        break;
    case HB_REPLY:
        auto it = server_entries.find(conn->get_peer_addr().to_host());
        if (it == server_entries.end()) {
            log_error("%s not found in server_entries", conn->get_peer_addr().to_host());
            break;
        }
        it->second->read_index_heartbeat_replies++;
        size_t commits = 0;
        for (auto& serv : server_entries) {
            if (serv.second->read_index_heartbeat_replies == read_index_heartbeats) {
                commits++;
            }
        }
        // 如果收到了大多数节点的心跳响应，就说明自己还是leader
        if (commits >= getMajority() - 1) {
            apply_read_index = true;
        }
        break;
    }
}

void ServerNode::sendLogEntrySuccessfully(const angel::connection_ptr& conn)
{
    auto it = server_entries.find(conn->get_peer_addr().to_host());
    if (it == server_entries.end()) {
        log_error("%s not found in server_entries", conn->get_peer_addr().to_host());
        return;
    }
    it->second->next_index = logs.end();
    it->second->match_index = it->second->next_index;
    // If there exists an N such that N > commitIndex,
    // a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
    // set commitIndex = N
    //
    // Raft从来不会通过计算复制的数目来提交之前任期的日志条目
    // 只有领导人当前任期的日志条目才能通过计算复制数目来进行提交
    // 否则可能会出现：一条复制到了大多数服务器上的日志(还没提交)被新任领导人覆盖掉
    //
    if (config_change_state) {
        commitConfigLogEntry();
        return;
    }
    std::unordered_map<size_t, int> commit_map;
    for (auto& serv : server_entries) {
        commit_map[serv.second->match_index]++;
    }
    for (auto& [n, commits] : commit_map) {
        if (n <= commit_index) continue;
        // 如果一条日志复制到了大多数机器上，则称为可提交的
        if (commits >= getMajority() - 1 && logs[n - 1].term == current_term) {
            commit_index = n;
            break;
        }
    }
}

// 失败的话，递减next_index，并重新发送之后的所有日志条目
void ServerNode::sendLogEntryFail(const angel::connection_ptr& conn)
{
    auto it = server_entries.find(conn->get_peer_addr().to_host());
    assert(it != server_entries.end());
    if (it->second->next_index > 0) it->second->next_index--;
    sendLogEntry(it->second.get());
}

// Cold,new阶段，一条日志需要同时被新旧配置的多数派节点接收才能被提交
// Cnew阶段，只需要被新配置的多数派节点接收即可
void ServerNode::commitConfigLogEntry()
{
    std::unordered_map<size_t, int> commit_map;
    if (config_change_state == OLD_NEW_CONFIG) {
        for (auto& serv : server_entries) {
            if (old_config_nodes.count(serv.first)) {
                commit_map[serv.second->match_index]++;
            }
        }
        bool old_commit = false;
        size_t old_majority = getOldMajority();
        for (auto& [n, commits] : commit_map) {
            if (n <= commit_index) continue;
            if (commits >= old_majority && logs[n - 1].term == current_term) {
                old_commit = true;
                break;
            }
        }
        if (!old_commit) return;
        commit_map.clear();
    }
    for (auto& serv : server_entries) {
        if (new_config_nodes.count(serv.first)) {
            commit_map[serv.second->match_index]++;
        }
    }
    bool new_commit = false;
    size_t new_majority = getNewMajority();
    for (auto& [n, commits] : commit_map) {
        if (n <= commit_index) continue;
        if (commits >= new_majority && logs[n - 1].term == current_term) {
            commit_index = n;
            new_commit = true;
            break;
        }
    }
    if (!new_commit) return;
    if (config_change_state == OLD_NEW_CONFIG) {
        // 提交一条Cnew日志
        // (Cold,new中已经携带了相关配置信息)
        std::string config(1, logtype.new_config);
        logs.append(current_term, config);
        config_change_state = NEW_CONFIG;
        info("append a Cnew log[%zu](%zu)", logs.end() - 1, current_term);
        sendLogEntry();
    } else {
        applyNewConfig();
        completeConfigChange();
    }
}

void ServerNode::applyNewConfig()
{
    // 移除不在新配置中的节点
    for (auto it = server_entries.begin(); it != server_entries.end(); ) {
        if (!new_config_nodes.count(it->first)) {
            it = server_entries.erase(it);
        } else {
            ++it;
        }
    }
}

void ServerNode::updateConfigFile(std::unordered_set<std::string>& config_nodes)
{
    char buf[1024];
    bool update = false;
    FILE *fp = fopen(rconf.confile.c_str(), "r");
    auto tmpfile = getTmpFile();
    int fd = open(tmpfile.c_str(), O_RDWR | O_CREAT, 0644);
    while (fgets(buf, sizeof(buf), fp)) {
        if (strlen(buf) >= 4 && strncmp(buf, "node", 4) == 0) {
            if (update) continue;
            for (auto& host : config_nodes) {
                std::string line("node ");
                auto p = std::find(host.begin(), host.end(), ':');
                std::string ip(host.begin(), p);
                std::string port(p + 1, host.end());
                line.append(ip).append(" ").append(port).append("\n");
                write(fd, line.data(), line.size());
            }
            update = true;
        } else {
            write(fd, buf, strlen(buf));
        }
    }
    fclose(fp);
    fsync(fd);
    close(fd);
    rename(tmpfile.c_str(), rconf.confile.c_str());
}

void ServerNode::completeConfigChange()
{
    info("cluster member change completed");
    // 不在新配置中的节点就可以退出了
    if (!new_config_nodes.count(self_host)) {
        info("I'm not in the new configuration, ready to quit...");
        server_entries.clear();
        loop->quit();
    }
    updateConfigFile(new_config_nodes);
    cancelConfigChangeTimer();
    clearConfigChangeInfo();
}

void ServerNode::clearConfigChangeInfo()
{
    old_config_nodes.clear();
    new_config_nodes.clear();
    config_change_state = 0;
}

void ServerNode::processRpcAsCandidate(const angel::connection_ptr& conn, rpc& r)
{
    switch (r.gettype()) {
    case HB_RPC:
        // 别的候选人赢得了选举
        if (r.ae().leader_term == current_term) {
            clearCandidateInfo();
            role = FOLLOWER;
        }
        updateLastRecvHeartbeatTime();
        break;
    case RV_REPLY:
        if (!r.reply().success) break;
        if (config_change_state == OLD_NEW_CONFIG) {
            // 需要同时获得新旧配置的多数派的投票才行
            auto host = conn->get_peer_addr().to_host();
            if (old_config_nodes.count(host)) old_votes++;
            if (new_config_nodes.count(host)) new_votes++;
            if (old_votes >= getOldMajority() && new_votes >= getNewMajority()) {
                becomeNewLeader();
            }
        } else {
            // 统计票数，如果获得大多数服务器的投票，则当选为新的领导人
            if (++votes >= getMajority()) {
                becomeNewLeader();
            }
        }
        break;
    }
}

void ServerNode::becomeNewLeader()
{
    role = LEADER;
    info("become a new leader");
    cancelElectionTimer();
    setHeartBeatTimer();
    for (auto& serv : server_entries) {
        serv.second->next_index = logs.end();
        serv.second->match_index = 0;
        serv.second->read_index_heartbeat_replies = 0;
    }
    apply_read_index = false;
    read_index_heartbeats = 0;
    // 提交一条空操作日志
    // (为了知道哪些日志是已被提交的，以及尝试提交之前还未被提交的日志)
    std::string noop(1, logtype.noop);
    logs.append(current_term, noop);
    info("append an empty log[%zu](%zu)", logs.end() - 1, current_term);
    sendLogEntry();
    if (config_change_state) {
        setConfigChangeTimer();
    }
}

void ServerNode::processRpcAsFollower(const angel::connection_ptr& conn, rpc& r)
{
    if (rconf.learner) rconf.learner = false;
    switch (r.gettype()) {
    case AE_RPC: recvLogEntry(conn, r.ae()); break;
    case RV_RPC: votedForCandidate(conn, r.rv()); break;
    case IS_RPC: recvSnapshot(conn, r.snapshot()); break;
    case HB_RPC:
        updateRecentLeader(r.ae().leader_id);
        updateLastRecvHeartbeatTime();
        updateCommitIndex(r.ae());
        if (r.ae().prev_log_term == hbtype.read_index)
            sendReply(conn, HB_REPLY, true);
        break;
    }
}

void ServerNode::recvLogEntry(const angel::connection_ptr& conn, AppendEntry& ae)
{
    size_t new_log_index = 0;
    for (const auto& log : ae.logs) {
        if (ae.prev_log_term > 0) {
            // 进行一致性检查(如果上一条日志不匹配，就返回false)
            if (ae.prev_log_index >= logs.end()) {
                goto fail;
            }
            if (ae.prev_log_index < logs.baseIndex() &&
                    (ae.prev_log_index != last_included_index || ae.prev_log_term != last_included_term)) {
                goto fail;
            }
            if (ae.prev_log_index >= logs.baseIndex() && logs[ae.prev_log_index].term != ae.prev_log_term) {
                goto fail;
            }
            new_log_index = ae.prev_log_index + 1;
        }
        // 如果一条已经存在的日志与新的日志冲突（index相同但是term不同），
        // 就删除已经存在的日志和它之后所有的日志
        if (new_log_index < logs.end()) {
            if (logs[new_log_index].term != log.term) {
                logs.remove(new_log_index);
            } else { // 该条日志已存在
                goto next;
            }
        }
        // 追加新日志
        logs.append(log.term, log.cmd);
        info("append a new log[%zu](%zu)", logs.end() - 1, log.term);
        recvConfigLogEntry(log);
next:
        if (ae.prev_log_term > 0) ae.prev_log_index++;
        ae.prev_log_term = log.term;
    }
    updateCommitIndex(ae);
    sendReply(conn, AE_REPLY, true);
    return;
fail:
    sendReply(conn, AE_REPLY, false);
}

void ServerNode::recvConfigLogEntry(const LogEntry& log)
{
    switch (log.cmd.back()) {
    case logtype.old_new_config:
        recvOldNewConfigLogEntry(log);
        break;
    case logtype.new_config:
        recvNewConfigLogEntry(log);
        break;
    case logtype.old_config:
        recvOldConfigLogEntry(log);
        break;
    }
}

void ServerNode::recvOldNewConfigLogEntry(const LogEntry& log)
{
    auto s = log.cmd.begin();
    auto es = log.cmd.end() - 1;
    auto p = std::find(s, es, ';');
    parseNodes(old_config_nodes, s, p);
    parseNodes(new_config_nodes, p + 1, es);
    applyOldNewConfig();
    info("recvd a Cold,new log[%zu](%zu) <%s>", logs.end() - 1, log.term, log.cmd.c_str());
    config_change_state = OLD_NEW_CONFIG;
}

void ServerNode::recvNewConfigLogEntry(const LogEntry& log)
{
    if (config_change_state != OLD_NEW_CONFIG) return;
    info("recvd a Cnew log[%zu](%zu)", logs.end() - 1, log.term);
    applyNewConfig();
    config_change_state = NEW_CONFIG;
}

void ServerNode::recvOldConfigLogEntry(const LogEntry& log)
{
    if (!config_change_state) return;
    info("recvd a Cold log[%zu](%zu)", logs.end() - 1, log.term);
    rollbackOldConfig();
    clearConfigChangeInfo();
}

void ServerNode::updateCommitIndex(AppendEntry& ae)
{
    if (ae.leader_commit > commit_index) {
        commit_index = std::min(ae.leader_commit, logs.end());
    }
}

// 领导人完全原则：一个新的领导人一定拥有已被提交的所有日志条目
//
// 因为一个候选人要想成为领导人，就必须获得集群中大多数节点(s1)的投票，
// 而一个节点只有当候选人的日志至少和自己一样新时才会投给他。
// 这意味着，只要有一条日志被提交了，即复制到了大多数节点上(s2)，那么s1与s2就必然至少拥有一个交集，
// 即s2中至少有一个节点会给新的领导人投票。
//
void ServerNode::votedForCandidate(const angel::connection_ptr& conn, RequestVote& rv)
{
    if ((voted_for.empty() || voted_for == rv.candidate_id)) {
        if (logUpToDate(rv)) {
            voted_for = rv.candidate_id;
            sendReply(conn, RV_REPLY, true);
            info("voted for %s", voted_for.c_str());
            updateLastRecvHeartbeatTime();
            saveState();
            return;
        }
    }
    sendReply(conn, RV_REPLY, false);
}

// Raft通过比较日志中最后一个条目的索引和任期号来决定两个日志哪一个更新。
// 如果两个日志的任期号不同，则任期号大的更新；如果任期号相同，则更长的日志更新。
//
// 候选人的日志至少和自己的一样新
bool ServerNode::logUpToDate(RequestVote& rv)
{
    // 候选人日志为空
    if (rv.last_log_term == 0) return logs.empty();
    if (logs.empty()) return true;
    if (rv.last_log_term == logs.lastTerm()) {
        return rv.last_log_index >= logs.end() - 1;
    } else {
        return rv.last_log_term > logs.lastTerm();
    }
}

void ServerNode::clearCandidateInfo()
{
    cancelElectionTimer();
    voted_for.clear();
    saveState();
}

void ServerNode::clearLeaderInfo()
{
    cancelHeartBeatTimer();
    voted_for.clear();
    saveState();
}

void ServerNode::clearFollowerInfo()
{
    voted_for.clear();
    saveState();
}

void ServerNode::serverCron()
{
    if (rconf.learner) {
        updateLastRecvHeartbeatTime();
    } else if (role == FOLLOWER) {
        auto now = angel::util::get_cur_time_ms();
        if (now - last_recv_heartbeat_time > timeout) {
            startLeaderElection();
        }
    }
    if (rconf.snapshot_threshold > 0) {
        if (last_applied - logs.baseIndex() >= rconf.snapshot_threshold) {
            saveSnapshot();
        }
    }
    if (service->isSavedSnapshot()) {
        logs.removeBefore(last_included_index);
        close(snapshot_fd);
        rename(snapshot_tmpfile.c_str(), rconf.snapshot.c_str());
        logs.setLastLog(last_included_index, last_included_term);
        snapshot_state = 0;
        info("saved snapshot [index: %zu, term: %zu]", last_included_index, last_included_term);
    }
}

// 领导人会周期性地向其他服务器发送心跳包以维持自己的领导地位
void ServerNode::setHeartBeatTimer()
{
    heartbeat_timer_id = loop->run_every(rconf.heartbeat_period, [this]{
            this->sendHeartBeat(hbtype.normal);
            });
}

void ServerNode::sendHeartBeat(int type)
{
    for (auto& serv : server_entries) {
        if (serv.second->client->is_connected()) {
            auto& conn = serv.second->client->conn();
            conn->format_send("%s,%zu,%s,%zu,%zu,%zu\r\n", rpcts.hb_rpc,
                    current_term, run_id.c_str(), 0, type, commit_index);
        }
    }
}

void ServerNode::cancelHeartBeatTimer()
{
    loop->cancel_timer(heartbeat_timer_id);
}

// 发起新一轮选举
void ServerNode::startLeaderElection()
{
    role = CANDIDATE;
    ++current_term;
    voted_for = run_id;
    votes = 1; // 先给自己投上一票
    if (config_change_state == OLD_NEW_CONFIG) {
        old_votes = new_votes = 0;
    }
    info("start %zuth leader election", current_term);
    setElectionTimer();
    requestServersToVote();
    saveState();
}

void ServerNode::setElectionTimer()
{
    timeout = getElectionTimeout();
    election_timer_id = loop->run_after(timeout, [this]{
            this->startLeaderElection();
            });
}

void ServerNode::cancelElectionTimer()
{
    loop->cancel_timer(election_timer_id);
}

int ServerNode::getElectionTimeout()
{
    ::srand(::time(nullptr));
    return rconf.election_timeout.base + ::rand() % rconf.election_timeout.range;
}

// 请求别的服务器给自己投票
void ServerNode::requestServersToVote()
{
    size_t last_log_index = logs.empty() ? 0 : logs.end() - 1;
    for (auto& serv : server_entries) {
        if (serv.second->client->is_connected()) {
            auto& conn = serv.second->client->conn();
            conn->format_send("%s,%zu,%s,%zu,%zu\r\n", rpcts.rv_rpc,
                    current_term, run_id.c_str(), last_log_index, logs.lastTerm());
        }
    }
}

// 持久化一些状态以保证节点重启后能正常运行
//
// 1) current_term(要保证严格递增)
// 2) logs(不言而喻)
// 3) voted_for(要保证一个任期内只能投一次票)
//
void ServerNode::saveState()
{
    int fd = open(rconf.statefile.c_str(), O_RDWR | O_TRUNC | O_CREAT, 0644);
    struct iovec iov[3];
    iov[0].iov_base = &current_term;
    iov[0].iov_len = sizeof(current_term);
    size_t size = voted_for.size();
    iov[1].iov_base = &size;
    iov[1].iov_len = sizeof(size);
    iov[2].iov_base = const_cast<char*>(voted_for.data());
    iov[2].iov_len = size;
    writev(fd, iov, 3);
    fsync(fd);
    close(fd);
}

void ServerNode::loadState()
{
    int fd = open(rconf.statefile.c_str(), O_RDONLY);
    if (fd > 0) {
        read(fd, &current_term, sizeof(current_term));
        size_t len;
        read(fd, &len, sizeof(len));
        voted_for.resize(len);
        read(fd, const_cast<char*>(voted_for.data()), len);
        close(fd);
    }
}

void ServerNode::recvSnapshot(const angel::connection_ptr& conn, InstallSnapshot& snapshot)
{
    if (snapshot_state == SAVE_SNAPSHOT) return;
    if (snapshot.offset == 0) {
        snapshot_tmpfile = getTmpFile();
        snapshot_fd = open(snapshot_tmpfile.c_str(), O_RDWR | O_CREAT, 0644);
        snapshot_state = RECV_SNAPSHOT;
    }
    info("recvd snapshot [index: %zu, term: %zu](off=%zu, size=%zu, done=%d)", snapshot.last_included_index,
            snapshot.last_included_term, snapshot.offset, snapshot.data.size(), snapshot.done);
    lseek(snapshot_fd, snapshot.offset, SEEK_SET);
    write(snapshot_fd, snapshot.data.data(), snapshot.data.size());
    updateLastRecvHeartbeatTime();
    if (!snapshot.done) {
        sendReply(conn, IS_REPLY, true);
        return;
    }
    fsync(snapshot_fd);
    close(snapshot_fd);
    rename(snapshot_tmpfile.c_str(), rconf.snapshot.c_str());
    snapshot_state = 0;

    last_included_index = snapshot.last_included_index;
    last_included_term = snapshot.last_included_term;
    if (last_included_index < logs.end()) {
        if (logs[last_included_index].term == last_included_term) {
            logs.removeBefore(last_included_index);
            sendReply(conn, IS_REPLY, true);
            return;
        }
    }
    logs.remove(logs.baseIndex());
    logs.setLastLog(last_included_index, last_included_term);
    // skip last_included_index, last_included_term
    service->loadSnapshot(rconf.snapshot, sizeof(size_t) * 2);
    info("snapshot [index: %zu, term: %zu] is applied to the state machine",
            snapshot.last_included_index, snapshot.last_included_term);
    last_applied = last_included_index + 1;
    sendReply(conn, IS_REPLY, true);
}

// mmap()的offset参数必须是pagesize(一般为4K)的整数倍
static const int pagesize = getpagesize();

#define CHUNK_SIZE (pagesize * 1024)

void ServerNode::sendSnapshot(const angel::connection_ptr& conn)
{
    int fd = open(rconf.snapshot.c_str(), O_RDONLY);
    off_t filesize = getFileSize(fd);

    size_t n = filesize / CHUNK_SIZE;
    size_t remain_bytes = filesize % CHUNK_SIZE;
    std::vector<size_t> chunks(n, CHUNK_SIZE);
    if (remain_bytes > 0) chunks.push_back(remain_bytes);

    size_t offset = 0;
    for (size_t i = 0; i < chunks.size(); i++) {
        size_t size = chunks[i];
        bool done = i == chunks.size() - 1 ? true : false;
        void *start = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, offset);
        conn->format_send("%s,%zu,%s,%zu,%zu,%zu,%d,%zu\r\n", rpcts.is_rpc, current_term,
                run_id.c_str(), last_included_index, last_included_term, offset, done, size);
        conn->send(start, size);
        munmap(start, size);
        offset += size;
    }
    info("send snapshot [index: %zu, term: %zu] to %s", last_included_index, last_included_term,
            conn->get_peer_addr().to_host());
    close(fd);
}

// 用快照取代已被应用到状态机的日志条目
void ServerNode::saveSnapshot()
{
    if (snapshot_state == SAVE_SNAPSHOT) return;
    if (snapshot_state == RECV_SNAPSHOT) return;
    if (last_applied == 0) return;
    snapshot_state = SAVE_SNAPSHOT;
    snapshot_tmpfile = getTmpFile();
    int fd = open(snapshot_tmpfile.c_str(), O_RDWR | O_CREAT, 0644);
    last_included_index = last_applied - 1;
    last_included_term = logs[last_included_index].term;
    struct iovec iov[2];
    iov[0].iov_base = &last_included_index;
    iov[0].iov_len = sizeof(last_included_index);
    iov[1].iov_base = &last_included_term;
    iov[1].iov_len = sizeof(last_included_term);
    writev(fd, iov, 2);
    close(fd);
    info("save snapshot... [index: %zu, term: %zu]", last_included_index, last_included_term);
    service->saveSnapshot(snapshot_tmpfile);
}

void ServerNode::loadSnapshot()
{
    int fd = open(rconf.snapshot.c_str(), O_RDONLY);
    if (fd < 0) return;
    read(fd, &last_included_index, sizeof(last_included_index));
    read(fd, &last_included_term, sizeof(last_included_term));
    close(fd);
    last_applied = last_included_index + 1;
    logs.setLastLog(last_included_index, last_included_term);
    service->loadSnapshot(rconf.snapshot, sizeof(size_t) * 2);
}

std::string ServerNode::generateRunid()
{
    return self_host;
}

void ServerNode::updateRecentLeader(const std::string& leader_id)
{
    recent_leader = leader_id;
}

void ServerNode::redirectToLeader(const angel::connection_ptr& conn)
{
    conn->format_send("<host>%s\r\n", recent_leader.c_str());
}

void ServerEntry::start(ServerNode *self)
{
    client->set_connection_handler([self, this](const angel::connection_ptr& conn){
            self->info("### connect with server %s", conn->get_peer_addr().to_host());
            // 尝试补发缺失的日志
            if (self->role == LEADER) {
                self->sendLogEntry(this);
            }
            });
    client->set_message_handler([self](const angel::connection_ptr& conn, angel::buffer& buf){
            self->processRpcFromServer(conn, buf);
            });
    client->set_close_handler([self](const angel::connection_ptr& conn){
            self->info("### disconnect with server %s", conn->get_peer_addr().to_host());
            });
    client->not_exit_loop();
    client->start();
}

void ServerNode::info(const char *fmt, ...)
{
    // Here you can disable the output.
	va_list		ap;
    char buf[4096];
    const char *rolestr;
    switch (role) {
    case LEADER: rolestr = "leader:    "; break;
    case CANDIDATE: rolestr = "candidate: "; break;
    case FOLLOWER: rolestr = "follower:  "; break;
    }
	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
    fprintf(stdout, "%s%s\n", rolestr, buf);
	va_end(ap);
}
