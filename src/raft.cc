#include "raft.h"
#include "config.h"

#include <iostream>
#include <random>

#include <time.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <assert.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdarg.h>

using namespace raft;

void ServerNode::initServer()
{
    run_id = generateRunid();
    info("my runid is %s", run_id.c_str());
    readConf(rconf.confile);
    connectNodes();
    setPaths();
    loop->run_every(rconf.server_cron_period, [this]{ this->serverCron(); });
    log_fd = open(rconf.logfile.c_str(), O_RDWR | O_APPEND | O_CREAT, 0644);
    loadState();
    loadLog();
}

void ServerNode::connectNodes()
{
    for (auto& node : rconf.nodes) {
        if (node.port == server.listen_addr().to_host_port())
            continue;
        auto se = new ServerEntry(loop, angel::inet_addr(node.ip.c_str(), node.port));
        se->start(this);
        server_entries.emplace(node.ip + ":" + std::to_string(node.port), se);
    }
}

void ServerNode::setPaths()
{
    mkdir("rlog", 0777);
    static char pid[64] = { 0 };
    snprintf(pid, sizeof(pid), "rlog/%s.log", run_id.c_str());
    rconf.logfile = pid;
    snprintf(pid, sizeof(pid), "rlog/%s.state", run_id.c_str());
    rconf.statefile = pid;
}

void ServerNode::process(const angel::connection_ptr& conn,
                         angel::buffer& buf)
{
    rpc r;
    while (buf.readable() > 2) {
        int crlf = buf.find_crlf();
        if (crlf < 0) break;
        if (buf.starts_with("<user>")) { // 客户端的请求
            if (role == LEADER) {
                std::string cmd(buf.peek() + 6, crlf - 6);
                if (!cmd.empty()) {
                    appendLogEntry(current_term, cmd);
                    clients.emplace(log_entries.size() - 1, conn->id());
                    sendLogEntry();
                }
            } else { // 重定向
                conn->format_send("<host>%s\r\n", recent_leader.c_str());
            }
        } else { // 内部通信
            r.parse(buf.peek(), buf.peek() + crlf);
            processrpc(conn, r);
        }
        buf.retrieve(crlf + 2);
    }
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
    for (size_t idx = serv->next_index; idx < log_entries.size(); idx++) {
        auto& next_log = log_entries[idx];
        size_t prev_log_index = 0, prev_log_term = 0;
        if (idx > 0) {
            prev_log_index = idx - 1;
            prev_log_term = log_entries[prev_log_index].leader_term;
        }
        auto& conn = serv->client->conn();
        conn->format_send("AE_RPC,%zu,%s,%zu,%zu,%zu,%zu,",
                current_term, run_id.c_str(), prev_log_index, prev_log_term,
                commit_index, next_log.leader_term);
        conn->send(next_log.cmd);
        conn->send("\r\n");
    }
}

void ServerNode::processRpcFromServer(const angel::connection_ptr& conn,
                                      angel::buffer& buf)
{
    rpc r;
    while (buf.readable() > 2) {
        int crlf = buf.find_crlf();
        if (crlf >= 0) {
            r.parse(buf.peek(), buf.peek() + crlf);
            processrpc(conn, r);
            buf.retrieve(crlf + 2);
        } else
            break;
    }
}

void ServerNode::processrpc(const angel::connection_ptr& conn, rpc& r)
{
    auto term = r.getterm();
    if (term < current_term) {
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
        auto& apply_log = log_entries[last_applied];
        if (apply_log.cmd != "")
            kv.execute(apply_log.cmd);
        info("log[%zu](%zu) is applied to the state machine",
                last_applied, apply_log.leader_term);
        if (role == LEADER) { // 回复客户端
            auto it = clients.find(last_applied);
            if (it != clients.end()) {
                auto conn = server.get_connection(it->second);
                if (conn->is_connected())
                    conn->send(kv.get_reply());
                clients.erase(it);
            }
        }
        ++last_applied;
    }
}

void ServerNode::processRpcAsLeader(const angel::connection_ptr& conn, rpc& r)
{
    switch (r.gettype()) {
    case AE_REPLY:
        if (r.reply().success) sendLogEntrySuccessfully(conn);
        else sendLogEntryFail(conn);
        break;
    }
}

void ServerNode::sendLogEntrySuccessfully(const angel::connection_ptr& conn)
{
    auto serv = getServerEntry(conn->get_peer_addr().to_host());
    assert(serv);
    serv->next_index++;
    serv->match_index = serv->next_index;
    // If there exists an N such that N > commitIndex,
    // a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
    // set commitIndex = N
    //
    // Raft从来不会通过计算复制的数目来提交之前任期的日志条目
    // 只有领导人当前任期的日志条目才能通过计算复制数目来进行提交
    // 否则可能会出现：一条复制到了大多数服务器上的日志(还没提交)被新任领导人覆盖掉
    //
    std::unordered_map<size_t, int> commit_map;
    for (auto& serv : server_entries) {
        commit_map[serv.second->match_index]++;
    }
    for (auto& [n, commits] : commit_map) {
        if (n <= commit_index) continue;
        // 如果一条日志复制到了大多数机器上，则称为可提交的
        if (commits >= getMajority() - 1 && log_entries[n - 1].leader_term == current_term) {
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
        // 统计票数，如果获得大多数服务器的投票，则当选为新的领导人
        if (r.reply().success && ++votes >= getMajority()) {
            becomeNewLeader();
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
        serv.second->next_index = log_entries.size();
        serv.second->match_index = 0;
    }
    // 提交一条空操作日志
    appendLogEntry(current_term, "");
    sendLogEntry();
}

void ServerNode::processRpcAsFollower(const angel::connection_ptr& conn, rpc& r)
{
    switch (r.gettype()) {
    case AE_RPC: recvLogEntryFromLeader(conn, r.ae()); break;
    case RV_RPC: votedForCandidate(conn, r.rv()); break;
    case HB_RPC:
        updateRecentLeader(r.ae().leader_id);
        updateLastRecvHeartbeatTime();
        updateCommitIndex(r.ae());
        break;
    }
}

void ServerNode::recvLogEntryFromLeader(const angel::connection_ptr& conn, AppendEntry& ae)
{
    if (ae.prev_log_term > 0) { // 进行一致性检查
        // 如果上一条日志不匹配，就返回false
        if (ae.prev_log_index >= log_entries.size() ||
                log_entries[ae.prev_log_index].leader_term != ae.prev_log_term) {
            conn->format_send("AE_REPLY,%zu,%d\r\n", current_term, 0);
            return;
        }
        // 如果一条已经存在的日志与新的日志冲突（index相同但是term不同），
        // 就删除已经存在的日志和它之后所有的日志
        int new_index = ae.prev_log_index + 1;
        if (new_index < log_entries.size()) {
            if (log_entries[new_index].leader_term != ae.log_entry.leader_term) {
                removeLogEntry(new_index);
            } else { // 该条日志已存在
                return;
            }
        }
    } else {
        // 第一条日志是否已存在
        assert(ae.prev_log_index == 0);
        if (log_entries.size() > 0 && log_entries[0].leader_term == ae.log_entry.leader_term)
            return;
    }
    // 追加新日志
    appendLogEntry(ae.log_entry.leader_term, ae.log_entry.cmd);
    updateCommitIndex(ae);
    conn->format_send("AE_REPLY,%zu,%d\r\n", current_term, 1);
}

void ServerNode::updateCommitIndex(AppendEntry& ae)
{
    if (ae.leader_commit > commit_index) {
        commit_index = std::min(ae.leader_commit, log_entries.size());
    }
}

void ServerNode::votedForCandidate(const angel::connection_ptr& conn, RequestVote& rv)
{
    updateLastRecvHeartbeatTime();
    if ((voted_for.empty() || voted_for == rv.candidate_id)) {
        if (logUpToDate(rv.last_log_index, rv.last_log_term)) {
            voted_for = rv.candidate_id;
            conn->format_send("RV_REPLY,%zu,1\r\n", current_term);
            info("voted for %s", voted_for.c_str());
            saveState();
            return;
        }
    }
    conn->format_send("RV_REPLY,%zu,0\r\n", current_term);
}

// 候选人的日志至少和自己的一样新
bool ServerNode::logUpToDate(size_t last_log_index, size_t last_log_term)
{
    // 候选人日志为空
    if (last_log_term == 0) return log_entries.empty();
    if (log_entries.empty()) return true;
    auto& last_log = log_entries.back();
    if (last_log_term == last_log.leader_term) {
        return last_log_index >= log_entries.size() - 1;
    } else {
        return last_log_term > last_log.leader_term;
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
    if (role == FOLLOWER) {
        auto now = angel::util::get_cur_time_ms();
        if (now - last_recv_heartbeat_time > rconf.election_timeout.base + rconf.election_timeout.range) {
            startLeaderElection();
        }
    }
}

// 领导人会周期性地向其他服务器发送心跳包以维持自己的领导地位
void ServerNode::setHeartBeatTimer()
{
    heartbeat_timer_id = loop->run_every(rconf.heartbeat_period, [this]{
            this->sendHeartBeat();
            });
}

void ServerNode::sendHeartBeat()
{
    size_t last_log_index = log_entries.size();
    for (auto& serv : server_entries) {
        if (serv.second->client->is_connected()) {
            auto& conn = serv.second->client->conn();
            if (last_log_index <= 1) {
                conn->format_send("HB_RPC,%zu,%s,0,0,%zu\r\n", current_term, run_id.c_str(), commit_index);
            } else {
                size_t prev_log_index = last_log_index - 2;
                size_t prev_log_term = log_entries[prev_log_index].leader_term;
                conn->format_send("HB_RPC,%zu,%s,%zu,%zu,%zu\r\n",
                        current_term, run_id.c_str(), prev_log_index, prev_log_term, commit_index);
            }
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
    info("start %zuth leader election", current_term);
    setElectionTimer();
    requestServersToVote();
    saveState();
}

void ServerNode::setElectionTimer()
{
    ::srand(::time(nullptr));
    time_t timeout = rconf.election_timeout.base + ::rand() % rconf.election_timeout.range;
    election_timer_id = loop->run_after(timeout, [this]{
            this->startLeaderElection();
            });
}

void ServerNode::cancelElectionTimer()
{
    loop->cancel_timer(election_timer_id);
}

// 请求别的服务器给自己投票
void ServerNode::requestServersToVote()
{
    size_t last_log_index = log_entries.size();
    for (auto& serv : server_entries) {
        if (serv.second->client->is_connected()) {
            auto& conn = serv.second->client->conn();
            if (last_log_index == 0) {
                conn->format_send("RV_RPC,%zu,%s,0,0\r\n", current_term, run_id.c_str());
            } else {
                size_t last_log_term = log_entries[--last_log_index].leader_term;
                conn->format_send("RV_RPC,%zu,%s,%zu,%zu\r\n",
                        current_term, run_id.c_str(), last_log_index, last_log_term);
            }
        }
    }
}

// [term][cmd-size][cmd]
void ServerNode::appendLogEntry(size_t term, const std::string& cmd)
{
    struct iovec iov[3];
    iov[0].iov_base = &term;
    iov[0].iov_len = sizeof(term);
    size_t size = cmd.size();
    iov[1].iov_base = &size;
    iov[1].iov_len = sizeof(size);
    iov[2].iov_base = const_cast<char*>(cmd.data());
    iov[2].iov_len = size;
    log_entries.emplace_back(term, cmd);
    writev(log_fd, iov, 3);
    fsync(log_fd);
    info("append new log[%zu](%zu)", log_entries.size() - 1, term);
}

void ServerNode::loadLog()
{
    auto filesize = getFileSize(log_fd);
    void *start = mmap(nullptr, filesize, PROT_READ, MAP_SHARED, log_fd, 0);
    if (start == MAP_FAILED) return;
    char *buf = reinterpret_cast<char*>(start);
    char *end = buf + filesize;
    while (buf < end) {
        size_t term = *reinterpret_cast<size_t*>(buf);
        buf += sizeof(term);
        size_t len = *reinterpret_cast<size_t*>(buf);
        buf += sizeof(len);
        std::string cmd(buf, len);
        buf += len;
        log_entries.emplace_back(term, cmd);
        info("load log[%zu](%zu)", log_entries.size() - 1, term);
    }
    munmap(start, filesize);
}

// 持久化current_term、voted_for以便节点重启后能够恢复之前的正常状态
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

void ServerNode::removeLogEntry(size_t from)
{
    off_t remove_bytes = 0;
    while (from < log_entries.size()) {
        auto& last = log_entries.back();
        remove_bytes += last.cmd.size() + sizeof(size_t) * 2;
        log_entries.pop_back();
    }
    off_t filesize = getFileSize(log_fd);
    off_t remain_bytes = filesize - remove_bytes;
    auto tmpfile = getTmpFile();
    int tmpfd = open(tmpfile.c_str(), O_RDWR | O_APPEND | O_CREAT, 0644);
    void *start = mmap(nullptr, remain_bytes, PROT_READ, MAP_SHARED, log_fd, 0);
    write(tmpfd, start, remain_bytes);
    fsync(tmpfd);
    close(tmpfd);
    munmap(start, remain_bytes);
    rename(tmpfile.c_str(), rconf.logfile.c_str());
}

std::string ServerNode::generateRunid()
{
    return server.listen_addr().to_host();
}

void ServerNode::updateRecentLeader(const std::string& leader_id)
{
    recent_leader = leader_id;
}

void ServerEntry::start(ServerNode *self)
{
    client->set_connection_handler([self](const angel::connection_ptr& conn){
            self->info("### connect with server %s", conn->get_peer_addr().to_host());
            // 尝试补发缺失的日志
            if (self->role == LEADER) {
                auto serv = self->getServerEntry(conn->get_peer_addr().to_host());
                assert(serv);
                self->sendLogEntry(serv);
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

off_t ServerNode::getFileSize(int fd)
{
    struct stat st;
    fstat(fd, &st);
    return st.st_size;
}

std::string ServerNode::getTmpFile()
{
    char tmpfile[32] = "tmp.XXXXXX";
    return mktemp(tmpfile);
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

int main(int argc, char *argv[])
{
    angel::set_log_level(angel::logger::level::info);
    angel::evloop loop;
    if (argc != 2) {
        printf("usage: serv [listen port]\n");
        return 1;
    }
    ServerNode node(&loop, angel::inet_addr("127.0.0.1", atoi(argv[1])));
    loop.run();
}
