#include "raft.h"

#include <random>
#include <time.h>
#include <iostream>

using namespace raft;

void ServerNode::processQueryFromClient(const Angel::TcpConnectionPtr& conn,
                                        Angel::Buffer& buf)
{
    if (buf.strcmp("user,")) {
        buf.retrieve(5);
        // do user commands
    } else {
        processRpcFromServer(conn, buf);
    }
}

void ServerNode::processRpcFromServer(const Angel::TcpConnectionPtr& conn,
                                      Angel::Buffer& buf)
{
    rpc r;
    while (buf.readable() > 2) {
        int crlf = buf.findCrlf();
        if (crlf >= 0) {
            parserpc(r, buf.begin(), buf.begin() + crlf);
            processRpcCommon(r);
            switch (role) {
            case leader: processRpcAsLeader(conn, r); break;
            case candidate: processRpcAsCandidate(conn, r); break;
            case follower: processRpcAsFollower(conn, r); break;
            }
            buf.retrieve(crlf + 2);
        } else
            break;
    }
}

void ServerNode::processRpcCommon(const rpc& r)
{
    if (commitIndex > lastApplied) {
        ++lastApplied;
        printf("log[%zu] is applied to the state machine\n", lastApplied);
    }
    size_t term = getterm(r);
    if (term > currentTerm) {
        currentTerm = term;
        if (role == candidate) {
            cancelElectionTimer();
            votedFor.clear();
            votes = 0;
        }
        if (role == leader) {
            cancelHeartBeatTimer();
        }
        role = follower;
        printf("convert to follower\n");
    }
}

void ServerNode::processRpcAsLeader(const Angel::TcpConnectionPtr& conn,
                                    const rpc& r)
{

}

void ServerNode::processRpcAsCandidate(const Angel::TcpConnectionPtr& conn,
                                       const rpc& r)
{
    // 别的候选人赢得了选举
    if (r.type == hb_rpc) {
        // 如果对方的任期比自己的大，则承认其合法
        if (r.msg.ae.leaderTerm > currentTerm) {
            role = follower;
            cancelElectionTimer();
            votedFor.clear();
            votes = 0;
        }
        lastRecvHeartBeatTime = Angel::nowMs();
        return;
    }
    // 统计票数，如果获得大多数服务器的投票，则当选为新的领导人
    if (r.type != rv_reply) return;
    size_t half = (serverEntries.size() + 1) / 2 + 1;
    if (r.msg.reply.success && ++votes >= half) {
        role = leader;
        cancelElectionTimer();
        setHeartBeatTimer();
        printf("become a new leader\n");
    }
}

void ServerNode::processRpcAsFollower(const Angel::TcpConnectionPtr& conn,
                                      const rpc& r)
{
    switch (r.type) {
    case ae_rpc:
        break;
    case rv_rpc:
        if (votedFor.empty()
                && logAsNewAsCandidate(r.msg.rv.lastLogIndex, r.msg.rv.lastLogTerm)) {
            votedFor = r.msg.rv.candidateId;
            conn->formatSend("rv_reply,%zu,1\r\n", currentTerm);
            printf("voted for %s\n", votedFor.c_str());
        }
        break;
    case hb_rpc:
        lastRecvHeartBeatTime = Angel::nowMs();
        break;
    }
}

bool ServerNode::logAsNewAsCandidate(size_t lastLogIndex, size_t lastLogTerm)
{
    return (lastLogTerm == 0 || logEntries.empty())
        || (logEntries.size() - 1 < lastLogIndex)
        || (logEntries[lastLogIndex].term == lastLogTerm);
}

void ServerNode::serverCron()
{
    if (role == follower) {
        time_t now = Angel::nowMs();
        if (now - lastRecvHeartBeatTime > hz.election_timeout)
            startLeaderElection();
    }
}

void ServerNode::setServerCronTimer()
{
    loop->runEvery(hz.period, [this]{ this->serverCron(); });
}

// 领导人会周期性地向其他服务器发送心跳包以维持自己的领导地位
void ServerNode::setHeartBeatTimer()
{
    heartbeatTimerId = loop->runEvery(hz.period, [this]{
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
    printf("start a new leader election\n");
}

void ServerNode::setElectionTimer()
{
    ::srand(::time(nullptr));
    // ~(150 ~ 300)ms
    time_t timeout = hz.election_timeout + (::rand() % 151 + 150);
    electionTimerId = loop->runAfter(timeout, [this]{
            printf("election timeout\n");
            this->electionTimerId = 0;
            this->startLeaderElection();
            });
}

void ServerNode::cancelElectionTimer()
{
    loop->cancelTimer(electionTimerId);
    electionTimerId = 0;
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
        const char *s = buf, *es = s + strlen(buf);
        const char *p = std::find(s, es, ':');
        if (p == es) error("parse conf error");
        ip.assign(s, p);
        port.assign(p + 1, es);
        if (atoi(port.c_str()) == server.listenAddr()->toIpPort())
            continue;
        auto entry = new ServerEntry;
        entry->client.reset(new Angel::TcpClient(loop, Angel::InetAddr(atoi(port.c_str()), ip.c_str())));
        entry->client->setMessageCb([this](const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf){
                this->processRpcFromServer(conn, buf);
                });
        entry->client->notExitLoop();
        entry->client->start();
        serverEntries.emplace(ip+":"+port, entry);
    }
    fclose(fp);
}

int main(int argc, char *argv[])
{
    Angel::EventLoop loop;
    ServerNode node(&loop, Angel::InetAddr(atoi(argv[1])));
    loop.run();
}
