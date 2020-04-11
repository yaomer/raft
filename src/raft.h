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

struct rconf {
    const size_t election_timeout = 3000;
    const size_t period = 300;
    const char *confile = "raft.conf";
    const char *logfile;
} rconf;

class ServerNode;

struct ServerEntry {
    ServerEntry(Angel::EventLoop *loop, Angel::InetAddr connAddr)
        : client(new Angel::TcpClient(loop, connAddr))
    {
    }
    void start(ServerNode *self);
    std::unique_ptr<Angel::TcpClient> client; // 到该服务器的连接
    size_t nextIndex = 0; // 发送给该服务器的下一条日志条目的索引
    // matchIndex=1表示logEntries[0]已经复制到该服务器上了
    size_t matchIndex = 0; // 已经复制到该服务器上的最大日志的索引
};

class ServerNode {
public:
    // 保存所有其他服务器，方便与之通信并维护必要的状态信息
    using ServerEntryMap = std::unordered_map<std::string, std::unique_ptr<ServerEntry>>;
    // 保存日志索引所对应的请求的客户端<logIndex, conn_id>
    // 在该日志条目被领导人执行后，会根据conn_id进行回复
    using Clients = std::unordered_map<size_t, size_t>;

    ServerNode(Angel::EventLoop *loop, Angel::InetAddr listenAddr)
        : server(loop, listenAddr)
    {
        this->loop = loop;
        initServer();
        server.setMessageCb([this](const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf){
                this->process(conn, buf);
                });
        server.start();
    }

    void initServer();
    void serverCron();

    void process(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf);
    void processRpcFromServer(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf);
    void processrpc(const Angel::TcpConnectionPtr& conn, rpc& r);
    void checkExpiredRpc(rpc& r);
    void applyLogEntry();
    void processRpcAsLeader(const Angel::TcpConnectionPtr& conn, rpc& r);
    void processRpcAsCandidate(const Angel::TcpConnectionPtr& conn, rpc& r);
    void processRpcAsFollower(const Angel::TcpConnectionPtr& conn, rpc& r);

    void sendLogEntryToServers();
    void recvLogEntryFromLeader(const Angel::TcpConnectionPtr& conn, rpc& r);
    void votedForCandidate(const Angel::TcpConnectionPtr& conn, rpc& r);

    void setHeartBeatTimer();
    void sendHeartBeatToServers();
    void cancelHeartBeatTimer();

    void startLeaderElection();
    void setElectionTimer();
    void cancelElectionTimer();
    void requestServersToVote();
    void becomeNewLeader();
private:
    bool logAsNewAsCandidate(size_t, size_t);
    void appendLogEntry(size_t term, const std::string& cmd);
    // remove logEntries[from...end] and sync changes to [logfile]
    void removeLogEntry(size_t from);
    void rebuildLogFile();
    void clearCandidateInfo();

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
    // commitIndex=1表示logEntries[0]已被提交了
    size_t commitIndex = 0; // 已被提交的最大日志条目的索引
    // lastApplied=1表示logEntries[0]已被应用到状态机了
    size_t lastApplied = 0; // 被状态机执行的最大日志条目的索引
    // 在leader服务器上不稳定存在的（赢得选举之后初始化）
    ServerEntryMap serverEntries;
    ////////////////////////////////////////////////////
    Role role = follower;
    Angel::EventLoop *loop;
    Angel::TcpServer server;
    // leader在接收客户端命令和回复响应时使用
    Clients clients;
    // 选举超时计时器和心跳计时器的id
    size_t electionTimerId = 0;
    size_t heartbeatTimerId = 0;
    // 最后一次收到心跳包的时间戳(ms)
    time_t lastRecvHeartBeatTime = Angel::nowMs();
    int logFd = -1;
    size_t half;
};

}

#endif // _RAFT_SRC_RAFT_H
