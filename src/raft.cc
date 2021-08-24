#include "raft.h"

#include <iostream>
#include <random>

#include <time.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <assert.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

using namespace raft;

void ServerNode::initServer()
{
    setRunId();
    readConf();
    mkdir("rlog", 0777);
    static char pid[64] = { 0 };
    snprintf(pid, sizeof(pid), "rlog/%s.log", run_id.c_str());
    rconf.logfile = pid;
    loop->run_every(rconf.period, [this]{ this->serverCron(); });
    log_fd = open(rconf.logfile, O_RDWR | O_APPEND | O_CREAT, 0644);
    half = (server_entries.size() + 1) / 2 + 1;
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
                appendLogEntry(current_term, std::string(buf.peek() + 6, crlf - 6));
                clients.emplace(log_entries.size() - 1, conn->id());
                sendLogEntryToServers();
            } else {
                // 重定向
                conn->send(">I'm not a leader<");
            }
        } else { // 内部通信
            parserpc(r, buf.peek(), buf.peek() + crlf);
            processrpc(conn, r);
        }
        buf.retrieve(crlf + 2);
    }
}

void ServerNode::sendLogEntryToServers()
{
    size_t last_log_index = log_entries.size();
    auto& le = log_entries[log_entries.size()-1];
    for (auto& serv : server_entries) {
        auto& client = serv.second->client;
        if (client->is_connected()) {
            if (last_log_index <= 1) {
                // 这里暂时不考虑cmd中包含二进制数据，假定都是ASCII字符
                client->conn()->format_send("AE_RPC,%zu,%s,%zu,%zu,%zu,%zu,%s\r\n",
                        current_term, run_id.c_str(), 0, 0, commit_index,
                        le.leader_term, le.cmd.c_str());
            } else {
                size_t prev_log_index = last_log_index - 2;
                size_t prev_log_term = log_entries[prev_log_index].leader_term;
                client->conn()->format_send("AE_RPC,%zu,%s,%zu,%zu,%zu,%zu,%s\r\n",
                        current_term, run_id.c_str(), prev_log_index, prev_log_term,
                        commit_index, le.leader_term, le.cmd.c_str());
            }
        }
    }
}

void ServerNode::processRpcFromServer(const angel::connection_ptr& conn,
                                      angel::buffer& buf)
{
    rpc r;
    while (buf.readable() > 2) {
        int crlf = buf.find_crlf();
        if (crlf >= 0) {
            parserpc(r, buf.peek(), buf.peek() + crlf);
            processrpc(conn, r);
            buf.retrieve(crlf + 2);
        } else
            break;
    }
}

void ServerNode::processrpc(const angel::connection_ptr& conn, rpc& r)
{
    checkExpiredRpc(r);
    switch (role) {
    case LEADER: processRpcAsLeader(conn, r); break;
    case CANDIDATE: processRpcAsCandidate(conn, r); break;
    case FOLLOWER: processRpcAsFollower(conn, r); break;
    }
    applyLogEntry();
}

void ServerNode::checkExpiredRpc(rpc& r)
{
    size_t term = getterm(r);
    if (term > current_term) {
        current_term = term;
        if (role == CANDIDATE) {
            clearCandidateInfo();
        }
        if (role == LEADER) {
            cancelHeartBeatTimer();
        }
        role = FOLLOWER;
        printf("convert to follower\n");
    }
}

void ServerNode::applyLogEntry()
{
    if (commit_index > last_applied) {
        printf("log[%zu] is applied to the state machine\n", last_applied);
        ++last_applied;
        if (role == LEADER) { // 回复客户端
            auto it = clients.find(last_applied-1);
            assert(it != clients.end());
            auto conn = server.get_connection(it->second);
            conn->format_send("<reply>%s", log_entries[last_applied-1].cmd.c_str());
            clients.erase(it);
        }
    }
}

void ServerNode::processRpcAsLeader(const angel::connection_ptr& conn, rpc& r)
{
    assert(r.type == AE_REPLY);
    char name[32] = { 0 };
    snprintf(name, sizeof(name), "%s:%d",
            conn->get_peer_addr().to_host_ip(), conn->get_peer_addr().to_host_port());
    auto it = server_entries.find(name);
    assert(it != server_entries.end());
    it->second->next_index++;
    it->second->match_index = it->second->next_index;
    size_t commits = 0;
    for (auto& serv : server_entries) {
        if (serv.second->match_index == commit_index + 1)
            commits++;
    }
    // 如果一条日志复制到了大多数机器上，则称为可提交的
    if (commits >= half) {
        commit_index++;
    }
}

void ServerNode::processRpcAsCandidate(const angel::connection_ptr& conn, rpc& r)
{
    // 别的候选人赢得了选举
    if (r.type == HB_RPC) {
        // 如果对方的任期比自己的大，则承认其合法
        if (r.ae().leader_term > current_term) {
            clearCandidateInfo();
            role = FOLLOWER;
        }
        last_recv_heartbeat_time = angel::util::get_cur_time_ms();
        return;
    }
    // 统计票数，如果获得大多数服务器的投票，则当选为新的领导人
    assert(r.type == RV_REPLY);
    if (r.reply().success && ++votes >= half) {
        becomeNewLeader();
        printf("become a new leader\n");
    }
}

void ServerNode::becomeNewLeader()
{
    role = LEADER;
    cancelElectionTimer();
    setHeartBeatTimer();
    for (auto& serv : server_entries) {
        serv.second->next_index = log_entries.size();
        serv.second->match_index = 0;
    }
}

void ServerNode::processRpcAsFollower(const angel::connection_ptr& conn, rpc& r)
{
    switch (r.type) {
    case AE_RPC: recvLogEntryFromLeader(conn, r); break;
    case RV_RPC: votedForCandidate(conn, r); break;
    case HB_RPC: last_recv_heartbeat_time = angel::util::get_cur_time_ms(); break;
    }
}

void ServerNode::recvLogEntryFromLeader(const angel::connection_ptr& conn, rpc& r)
{
    if (r.ae().leader_term < current_term) goto fail;
    if (r.ae().prev_log_term > 0) { // 进行一致性检查
        if (log_entries[r.ae().prev_log_index].leader_term != r.ae().prev_log_term) goto fail;
        if (log_entries.size() >= r.ae().prev_log_index + 2
                && log_entries[r.ae().prev_log_index+1].leader_term != r.ae().log_entry.leader_term) {
            removeLogEntry(r.ae().prev_log_index+1);
        }
    }
    appendLogEntry(r.ae().log_entry.leader_term, r.ae().log_entry.cmd);
    if (r.ae().leader_commit > commit_index) {
        commit_index = std::min(r.ae().leader_commit, log_entries.size()-1, std::less<size_t>());
    }
    conn->format_send("AE_REPLY,%zu,%d\r\n", current_term, 1);
    return;
fail:
    conn->format_send("AE_REPLY,%zu,%d\r\n", current_term, 0);
}

void ServerNode::votedForCandidate(const angel::connection_ptr& conn, rpc& r)
{
    if (voted_for.empty()
            && logAsNewAsCandidate(r.rv().last_log_index, r.rv().last_log_term)) {
        voted_for = r.rv().candidate_id;
        conn->format_send("RV_REPLY,%zu,1\r\n", current_term);
        printf("voted for %s\n", voted_for.c_str());
    }
}

bool ServerNode::logAsNewAsCandidate(size_t last_log_index, size_t last_log_term)
{
    return (last_log_term == 0 || log_entries.empty())
        || (log_entries.size() - 1 < last_log_index)
        || (log_entries[last_log_index].leader_term == last_log_term);
}

void ServerNode::clearCandidateInfo()
{
    cancelElectionTimer();
    voted_for.clear();
    votes = 0;
}

void ServerNode::serverCron()
{
    if (role == FOLLOWER) {
        time_t now = angel::util::get_cur_time_ms();
        if (now - last_recv_heartbeat_time > rconf.election_timeout)
            startLeaderElection();
    }
}

// 领导人会周期性地向其他服务器发送心跳包以维持自己的领导地位
void ServerNode::setHeartBeatTimer()
{
    heartbeat_timer_id = loop->run_every(rconf.period, [this]{
            this->sendHeartBeatToServers();
            });
}

void ServerNode::sendHeartBeatToServers()
{
    size_t last_log_index = log_entries.size();
    for (auto& serv : server_entries) {
        if (serv.second->client->is_connected()) {
            auto& conn = serv.second->client->conn();
            if (last_log_index <= 1) {
                // 没有日志或只有一条日志，此时让对方忽略一致性检查
                conn->format_send("HB_RPC,%zu,%s,0,0,%zu\r\n",
                        current_term, run_id.c_str(), commit_index);
            } else {
                size_t prev_log_index = last_log_index - 2;
                size_t prev_log_term = log_entries[prev_log_index].leader_term;
                conn->format_send("HB_RPC,%zu,%s,%zu,%zu\r\n",
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
    // 在这轮选举结束前不能再发起选举
    if (election_timer_id > 0) return;
    role = CANDIDATE;
    ++current_term;
    votes = 1; // 先给自己投上一票
    setElectionTimer();
    requestServersToVote();
    printf("start %zuth leader election\n", current_term);
}

void ServerNode::setElectionTimer()
{
    ::srand(::time(nullptr));
    // ~(150 ~ 300)ms
    time_t timeout = rconf.election_timeout + (::rand() % 151 + 150);
    election_timer_id = loop->run_after(timeout, [this]{
            this->election_timer_id = 0;
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

void ServerNode::appendLogEntry(size_t term, const std::string& cmd)
{
    struct iovec iov[3];
    iov[0].iov_base = &term;
    iov[0].iov_len = sizeof(term);
    iov[1].iov_base = const_cast<char*>(&cmd[0]);
    iov[1].iov_len = cmd.size();
    iov[2].iov_base = const_cast<char*>("\r\n");
    iov[2].iov_len = 2;
    log_entries.emplace_back(current_term, cmd);
    writev(log_fd, iov, 3);
    fsync(log_fd);
}

void ServerNode::removeLogEntry(size_t from)
{
    printf("remove log_entries from %zu\n", from);
    size_t end = log_entries.size();
    for (size_t i = from; i < end; i++)
        log_entries.pop_back();
    rebuildLogFile();
}

// 为了移除[logfile]中[from]之后的所有日志，我们创建一个新的文件，
// 将[from]之前的日志写入到新文件中，然后rename()原子的替换原文件，
// 我们把这个过程称为[rebuild]
// 很显然，当日志文件很大时，这种方法就很不合适了
// 一种简单的处理是：将日志写到多个小文件中，这样[rebuild]的代价就很小了
void ServerNode::rebuildLogFile()
{
    char tmpfile[32] = "tmp.XXXXXX";
    mktemp(tmpfile);
    int tmpFd = open(tmpfile, O_RDWR | O_APPEND | O_CREAT, 0644);
    struct stat st;
    fstat(log_fd, &st);
    size_t rebuildlogs = log_entries.size();
    char *start = static_cast<char*>(
            mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, log_fd, 0));
    const char *s = start;
    const char *es = s + st.st_size;
    const char *sep = "\r\n";
    const char *p;
    while (true) {
        p = std::search(s, es, sep, sep + 2);
        assert(p != es);
        if (--rebuildlogs == 0) break;
        s = p + 1;
    }
    write(tmpFd, start, p+2-start);
    fsync(tmpFd);
    close(tmpFd);
    munmap(start, st.st_size);
    rename(tmpfile, rconf.logfile);
}

void ServerNode::setRunId()
{
    run_id.reserve(33);
    char *buf = &run_id[0];
    struct timespec tsp;
    clock_gettime(_CLOCK_REALTIME, &tsp);
    std::uniform_int_distribution<size_t> u;
    std::mt19937_64 e(tsp.tv_sec * 1000000000 + tsp.tv_nsec);
    snprintf(buf, 17, "%lx", u(e));
    snprintf(buf + 16, 17, "%lx", u(e));
}

void ServerEntry::start(ServerNode *self)
{
    client->set_message_handler([self](const angel::connection_ptr& conn, angel::buffer& buf){
            self->processRpcFromServer(conn, buf);
            });
    client->not_exit_loop();
    client->start();
}

#define error(str) \
    { fprintf(stderr, str"\n"); abort(); }

void ServerNode::readConf()
{
    char buf[1024];
    FILE *fp = fopen("raft.conf", "r");
    if (!fp) error("can't open raft.conf");
    while (::fgets(buf, sizeof(buf), fp)) {
        if (buf[0] == '#') continue;
        std::string ip, port;
        const char *s = buf, *es = s + strlen(buf) - 1;
        const char *p = std::find(s, es, ':');
        if (p == es) error("parse conf error");
        ip.assign(s, p);
        port.assign(p + 1, es);
        if (atoi(port.c_str()) == server.listen_addr().to_host_port())
            continue;
        auto se = new ServerEntry(loop, angel::inet_addr(ip.c_str(), atoi(port.c_str())));
        se->start(this);
        server_entries.emplace(ip+":"+port, se);
    }
    fclose(fp);
}

int main(int argc, char *argv[])
{
    angel::evloop loop;
    if (argc != 2) {
        printf("usage: serv [listen port]\n");
        return 1;
    }
    ServerNode node(&loop, angel::inet_addr(atoi(argv[1])));
    loop.run();
}
