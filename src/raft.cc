#include "raft.h"
#include "config.h"
#include "util.h"

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

void ServerNode::initServer(const std::string& confile)
{
    rconf.confile = confile;
    run_id = generateRunid();
    info("my runid is %s", run_id.c_str());
    readConf(rconf.confile);
    connectNodes();
    setPaths();
    loadState();
    logs.load();
    loadSnapshot();
    loop->run_every(rconf.server_cron_period, [this]{ this->serverCron(); });
}

void ServerNode::connectNodes()
{
    for (auto& node : rconf.nodes) {
        if (node.host == server.listen_addr().to_host())
            continue;
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

void ServerNode::process(const angel::connection_ptr& conn, angel::buffer& buf)
{
    rpc r;
    while (buf.readable() > 2) {
        int crlf = buf.find_crlf();
        if (crlf < 0) break;
        if (buf.starts_with("<user>")) { // 客户端的请求
            if (role == LEADER) {
                std::string cmd(buf.peek() + 6, crlf - 6);
                if (!cmd.empty()) {
                    logs.append(current_term, cmd);
                    info("append a new log[%zu](%zu)", logs.end() - 1, current_term);
                    clients.emplace(logs.end() - 1, conn->id());
                    sendLogEntry();
                }
            } else { // 重定向
                conn->format_send("<host>%s\r\n", recent_leader.c_str());
            }
            buf.retrieve(crlf + 2);
        } else { // 内部通信
            r.parse(buf, crlf);
            if (r.gettype() == NONE) break;
            processrpc(conn, r);
        }
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
    conn->format_send("AE_RPC,%zu,%s,%zu,%zu,%zu,%zu\r\n", current_term, run_id.c_str(),
            prev_log_index, prev_log_term, commit_index, buffer.size());
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
        if (apply_log.cmd != "")
            service->apply(apply_log.cmd);
        info("log[%zu](%zu) is applied to the state machine", last_applied, apply_log.term);
        if (role == LEADER) { // 回复客户端
            auto it = clients.find(last_applied);
            if (it != clients.end()) {
                auto conn = server.get_connection(it->second);
                if (conn && conn->is_connected())
                    conn->send(service->reply());
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
    serv->next_index = logs.end();
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
        serv.second->next_index = logs.end();
        serv.second->match_index = 0;
    }
    // 提交一条空操作日志
    // (为了知道哪些日志是已被提交的，以及尝试提交之前还未被提交的日志)
    logs.append(current_term, "");
    info("append an empty log[%zu](%zu)", logs.end() - 1, current_term);
    sendLogEntry();
}

void ServerNode::processRpcAsFollower(const angel::connection_ptr& conn, rpc& r)
{
    switch (r.gettype()) {
    case AE_RPC: recvLogEntry(conn, r.ae()); break;
    case RV_RPC: votedForCandidate(conn, r.rv()); break;
    case IS_RPC: recvSnapshot(conn, r.snapshot()); break;
    case HB_RPC:
        updateRecentLeader(r.ae().leader_id);
        updateLastRecvHeartbeatTime();
        updateCommitIndex(r.ae());
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
    if (role == FOLLOWER) {
        auto now = angel::util::get_cur_time_ms();
        if (now - last_recv_heartbeat_time > rconf.election_timeout.base + rconf.election_timeout.range) {
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
            this->sendHeartBeat();
            });
}

void ServerNode::sendHeartBeat()
{
    for (auto& serv : server_entries) {
        if (serv.second->client->is_connected()) {
            auto& conn = serv.second->client->conn();
            // 目前HB_RPC并没有用到prev_log_index和prev_log_term
            conn->format_send("HB_RPC,%zu,%s,%zu,%zu,%zu\r\n",
                    current_term, run_id.c_str(), 0, 0, commit_index);
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
    size_t last_log_index = logs.empty() ? 0 : logs.end() - 1;
    for (auto& serv : server_entries) {
        if (serv.second->client->is_connected()) {
            auto& conn = serv.second->client->conn();
            conn->format_send("RV_RPC,%zu,%s,%zu,%zu\r\n",
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
        conn->format_send("IS_RPC,%zu,%s,%zu,%zu,%zu,%d,%zu\r\n", current_term, run_id.c_str(),
                last_included_index, last_included_term, offset, done, size);
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
