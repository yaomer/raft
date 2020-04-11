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
    snprintf(pid, sizeof(pid), "rlog/%s.log", runId.c_str());
    rconf.logfile = pid;
    loop->runEvery(rconf.period, [this]{ this->serverCron(); });
    logFd = open(rconf.logfile, O_RDWR | O_APPEND | O_CREAT, 0644);
    half = (serverEntries.size() + 1) / 2 + 1;
}

void ServerNode::process(const Angel::TcpConnectionPtr& conn,
                         Angel::Buffer& buf)
{
    rpc r;
    while (buf.readable() > 2) {
        int crlf = buf.findCrlf();
        if (crlf < 0) break;
        if (buf.strcmp("<user>")) { // 客户端的请求
            if (role == leader) {
                appendLogEntry(currentTerm, std::string(buf.peek()+6, crlf-6));
                clients.emplace(logEntries.size()-1, conn->id());
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
    size_t lastLogIndex = logEntries.size();
    auto& le = logEntries[logEntries.size()-1];
    for (auto& serv : serverEntries) {
        auto& client = serv.second->client;
        if (client->isConnected()) {
            if (lastLogIndex <= 1) {
                // 这里暂时不考虑cmd中包含二进制数据，假定都是ASCII字符
                client->conn()->formatSend("ae_rpc,%zu,%s,%zu,%zu,%zu,%zu,%s\r\n",
                        currentTerm, runId.c_str(), 0, 0, commitIndex,
                        le.term, le.cmd.c_str());
            } else {
                size_t prevLogIndex = lastLogIndex - 2;
                size_t prevLogTerm = logEntries[prevLogIndex].term;
                client->conn()->formatSend("ae_rpc,%zu,%s,%zu,%zu,%zu,%zu,%s\r\n",
                        currentTerm, runId.c_str(), prevLogIndex, prevLogTerm,
                        commitIndex, le.term, le.cmd.c_str());
            }
        }
    }
}

void ServerNode::processRpcFromServer(const Angel::TcpConnectionPtr& conn,
                                      Angel::Buffer& buf)
{
    rpc r;
    while (buf.readable() > 2) {
        int crlf = buf.findCrlf();
        if (crlf >= 0) {
            parserpc(r, buf.peek(), buf.peek() + crlf);
            processrpc(conn, r);
            buf.retrieve(crlf + 2);
        } else
            break;
    }
}

void ServerNode::processrpc(const Angel::TcpConnectionPtr& conn, rpc& r)
{
    checkExpiredRpc(r);
    switch (role) {
    case leader: processRpcAsLeader(conn, r); break;
    case candidate: processRpcAsCandidate(conn, r); break;
    case follower: processRpcAsFollower(conn, r); break;
    }
    applyLogEntry();
}

void ServerNode::checkExpiredRpc(rpc& r)
{
    size_t term = getterm(r);
    if (term > currentTerm) {
        currentTerm = term;
        if (role == candidate) {
            clearCandidateInfo();
        }
        if (role == leader) {
            cancelHeartBeatTimer();
        }
        role = follower;
        printf("convert to follower\n");
    }
}

void ServerNode::applyLogEntry()
{
    if (commitIndex > lastApplied) {
        printf("log[%zu] is applied to the state machine\n", lastApplied);
        ++lastApplied;
        if (role == leader) { // 回复客户端
            auto it = clients.find(lastApplied-1);
            assert(it != clients.end());
            auto conn = server.getConnection(it->second);
            conn->formatSend("<reply>%s", logEntries[lastApplied-1].cmd.c_str());
            clients.erase(it);
        }
    }
}

void ServerNode::processRpcAsLeader(const Angel::TcpConnectionPtr& conn, rpc& r)
{
    assert(r.type == ae_reply);
    char name[32] = { 0 };
    snprintf(name, sizeof(name), "%s:%d",
            conn->peerAddr().toIpAddr(), conn->peerAddr().toIpPort());
    auto it = serverEntries.find(name);
    assert(it != serverEntries.end());
    it->second->nextIndex++;
    it->second->matchIndex = it->second->nextIndex;
    size_t commits = 0;
    for (auto& serv : serverEntries) {
        if (serv.second->matchIndex == commitIndex + 1)
            commits++;
    }
    // 如果一条日志复制到了大多数机器上，则称为可提交的
    if (commits >= half) {
        commitIndex++;
    }
}

void ServerNode::processRpcAsCandidate(const Angel::TcpConnectionPtr& conn, rpc& r)
{
    // 别的候选人赢得了选举
    if (r.type == hb_rpc) {
        // 如果对方的任期比自己的大，则承认其合法
        if (r.ae().leaderTerm > currentTerm) {
            clearCandidateInfo();
            role = follower;
        }
        lastRecvHeartBeatTime = Angel::nowMs();
        return;
    }
    // 统计票数，如果获得大多数服务器的投票，则当选为新的领导人
    assert(r.type == rv_reply);
    if (r.reply().success && ++votes >= half) {
        becomeNewLeader();
        printf("become a new leader\n");
    }
}

void ServerNode::becomeNewLeader()
{
    role = leader;
    cancelElectionTimer();
    setHeartBeatTimer();
    for (auto& serv : serverEntries) {
        serv.second->nextIndex = logEntries.size();
        serv.second->matchIndex = 0;
    }
}

void ServerNode::processRpcAsFollower(const Angel::TcpConnectionPtr& conn, rpc& r)
{
    switch (r.type) {
    case ae_rpc: recvLogEntryFromLeader(conn, r); break;
    case rv_rpc: votedForCandidate(conn, r); break;
    case hb_rpc: lastRecvHeartBeatTime = Angel::nowMs(); break;
    }
}

void ServerNode::recvLogEntryFromLeader(const Angel::TcpConnectionPtr& conn, rpc& r)
{
    if (r.ae().leaderTerm < currentTerm) goto fail;
    if (r.ae().prevLogTerm > 0) { // 进行一致性检查
        if (logEntries[r.ae().prevLogIndex].term != r.ae().prevLogTerm) goto fail;
        if (logEntries.size() >= r.ae().prevLogIndex + 2
                && logEntries[r.ae().prevLogIndex+1].term != r.ae().LogEntry.term) {
            removeLogEntry(r.ae().prevLogIndex+1);
        }
    }
    appendLogEntry(r.ae().LogEntry.term, r.ae().LogEntry.cmd);
    if (r.ae().leaderCommit > commitIndex) {
        commitIndex = std::min(r.ae().leaderCommit, logEntries.size()-1, std::less<size_t>());
    }
    conn->formatSend("ae_reply,%zu,%d\r\n", currentTerm, 1);
    return;
fail:
    conn->formatSend("ae_reply,%zu,%d\r\n", currentTerm, 0);
}

void ServerNode::votedForCandidate(const Angel::TcpConnectionPtr& conn, rpc& r)
{
    if (votedFor.empty()
            && logAsNewAsCandidate(r.rv().lastLogIndex, r.rv().lastLogTerm)) {
        votedFor = r.rv().candidateId;
        conn->formatSend("rv_reply,%zu,1\r\n", currentTerm);
        printf("voted for %s\n", votedFor.c_str());
    }
}

bool ServerNode::logAsNewAsCandidate(size_t lastLogIndex, size_t lastLogTerm)
{
    return (lastLogTerm == 0 || logEntries.empty())
        || (logEntries.size() - 1 < lastLogIndex)
        || (logEntries[lastLogIndex].term == lastLogTerm);
}

void ServerNode::clearCandidateInfo()
{
    cancelElectionTimer();
    votedFor.clear();
    votes = 0;
}

void ServerNode::serverCron()
{
    if (role == follower) {
        time_t now = Angel::nowMs();
        if (now - lastRecvHeartBeatTime > rconf.election_timeout)
            startLeaderElection();
    }
}

// 领导人会周期性地向其他服务器发送心跳包以维持自己的领导地位
void ServerNode::setHeartBeatTimer()
{
    heartbeatTimerId = loop->runEvery(rconf.period, [this]{
            this->sendHeartBeatToServers();
            });
}

void ServerNode::sendHeartBeatToServers()
{
    size_t lastLogIndex = logEntries.size();
    for (auto& serv : serverEntries) {
        if (serv.second->client->isConnected()) {
            auto& conn = serv.second->client->conn();
            if (lastLogIndex <= 1) {
                // 没有日志或只有一条日志，此时让对方忽略一致性检查
                conn->formatSend("hb_rpc,%zu,%s,0,0,%zu\r\n",
                        currentTerm, runId.c_str(), commitIndex);
            } else {
                size_t prevLogIndex = lastLogIndex - 2;
                size_t prevLogTerm = logEntries[prevLogIndex].term;
                conn->formatSend("hb_rpc,%zu,%s,%zu,%zu\r\n",
                        currentTerm, runId.c_str(), prevLogIndex, prevLogTerm, commitIndex);
            }
        }
    }
}

void ServerNode::cancelHeartBeatTimer()
{
    loop->cancelTimer(heartbeatTimerId);
}

// 发起新一轮选举
void ServerNode::startLeaderElection()
{
    // 在这轮选举结束前不能再发起选举
    if (electionTimerId > 0) return;
    role = candidate;
    ++currentTerm;
    votes = 1; // 先给自己投上一票
    setElectionTimer();
    requestServersToVote();
    printf("start %zuth leader election\n", currentTerm);
}

void ServerNode::setElectionTimer()
{
    ::srand(::time(nullptr));
    // ~(150 ~ 300)ms
    time_t timeout = rconf.election_timeout + (::rand() % 151 + 150);
    electionTimerId = loop->runAfter(timeout, [this]{
            this->electionTimerId = 0;
            this->startLeaderElection();
            });
}

void ServerNode::cancelElectionTimer()
{
    loop->cancelTimer(electionTimerId);
}

// 请求别的服务器给自己投票
void ServerNode::requestServersToVote()
{
    size_t lastLogIndex = logEntries.size();
    for (auto& serv : serverEntries) {
        if (serv.second->client->isConnected()) {
            auto& conn = serv.second->client->conn();
            if (lastLogIndex == 0) {
                conn->formatSend("rv_rpc,%zu,%s,0,0\r\n", currentTerm, runId.c_str());
            } else {
                size_t lastLogTerm = logEntries[--lastLogIndex].term;
                conn->formatSend("rv_rpc,%zu,%s,%zu,%zu\r\n",
                        currentTerm, runId.c_str(), lastLogIndex, lastLogTerm);
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
    logEntries.emplace_back(currentTerm, cmd);
    writev(logFd, iov, 3);
    fsync(logFd);
}

void ServerNode::removeLogEntry(size_t from)
{
    printf("remove logEntry from %zu\n", from);
    size_t end = logEntries.size();
    for (size_t i = from; i < end; i++)
        logEntries.pop_back();
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
    fstat(logFd, &st);
    size_t rebuildlogs = logEntries.size();
    char *start = static_cast<char*>(
            mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, logFd, 0));
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
    runId.reserve(33);
    char *buf = &runId[0];
    struct timespec tsp;
    clock_gettime(_CLOCK_REALTIME, &tsp);
    std::uniform_int_distribution<size_t> u;
    std::mt19937_64 e(tsp.tv_sec * 1000000000 + tsp.tv_nsec);
    snprintf(buf, 17, "%lx", u(e));
    snprintf(buf + 16, 17, "%lx", u(e));
}

void ServerEntry::start(ServerNode *self)
{
    client->setMessageCb([self](const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf){
            self->processRpcFromServer(conn, buf);
            });
    client->notExitLoop();
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
        if (atoi(port.c_str()) == server.listenAddr()->toIpPort())
            continue;
        auto se = new ServerEntry(loop, Angel::InetAddr(atoi(port.c_str()), ip.c_str()));
        se->start(this);
        serverEntries.emplace(ip+":"+port, se);
    }
    fclose(fp);
}

int main(int argc, char *argv[])
{
    Angel::EventLoop loop;
    ServerNode node(&loop, Angel::InetAddr(atoi(argv[1])));
    loop.run();
}
