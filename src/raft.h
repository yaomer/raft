#ifndef _RAFT_SRC_RAFT_H
#define _RAFT_SRC_RAFT_H

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

#include <Angel/EventLoop.h>
#include <Angel/TcpServer.h>
#include <Angel/TcpClient.h>

#include "log_entry.h"
#include "rpc.h"

namespace raft {

enum Role {
    follower,
    candidate,
    leader,
};

struct hz {
    const size_t election_timeout = 3000;
    const size_t period = 300;
} hz;

struct ServerEntry {
    ServerEntry() = default;
    std::unique_ptr<Angel::TcpClient> client; // 到该服务器的连接
    size_t nextIndex; // 发送给该服务器的下一条日志条目的索引
    size_t matchIndex; // 已经复制到该服务器上的最大日志的索引
};

class ServerNode {
public:
    using ServerEntryMap = std::unordered_map<std::string, std::unique_ptr<ServerEntry>>;

    ServerNode(Angel::EventLoop *loop, Angel::InetAddr listenAddr)
        : server(loop, listenAddr)
    {
        this->loop = loop;
        setRunId();
        readConf();
        server.setMessageCb([this](const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf){
                this->processQueryFromClient(conn, buf);
                });
        setServerCronTimer();
        server.start();
    }

    void serverCron();
    void setServerCronTimer();

    void processQueryFromClient(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf);
    void processRpcFromServer(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf);
    void processRpcCommon(const rpc& r);
    void processRpcAsLeader(const Angel::TcpConnectionPtr& conn, const rpc& r);
    void processRpcAsCandidate(const Angel::TcpConnectionPtr& conn, const rpc& r);
    void processRpcAsFollower(const Angel::TcpConnectionPtr& conn, const rpc& r);

    void setHeartBeatTimer();
    void sendHeartBeatToServers();
    void cancelHeartBeatTimer();

    void startLeaderElection();
    void setElectionTimer();
    void cancelElectionTimer();
    void requestServersToVote();
private:
    bool logAsNewAsCandidate(size_t, size_t);

    void setRunId();
    void readConf();

    // 在所有服务器上持久存在的
    std::string runId; // 服务器的运行时id（分布式唯一id）
    size_t currentTerm = 0; // 服务器的当前任期，单调递增
    std::string votedFor; // 当前任期内收到选票的候选人id（投给了谁）
    size_t votes; // 当前任期内收到了多少票数
    LogEntryList logEntries; // 要执行的日志条目
    // 在所有服务器上不稳定存在的
    // 如果一条日志被复制到了大多数服务器上，就称为‘可提交的‘
    size_t commitIndex = 0; // 已被提交的最大日志条目的索引
    size_t lastApplied = 0; // 被状态机执行的最大日志条目的索引
    // 在leader服务器上不稳定存在的（赢得选举之后初始化）
    ServerEntryMap serverEntries;
    ////////////////////////////////////////////////////
    Role role = follower;
    Angel::EventLoop *loop;
    Angel::TcpServer server;
    size_t electionTimerId = 0;
    size_t heartbeatTimerId = 0;
    time_t lastRecvHeartBeatTime = Angel::nowMs();
};

}

#endif // _RAFT_SRC_RAFT_H
