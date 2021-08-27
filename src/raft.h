#ifndef _RAFT_SRC_RAFT_H
#define _RAFT_SRC_RAFT_H

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

#include <angel/server.h>
#include <angel/client.h>

#include "rpc.h"

namespace raft {

enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER,
};

class ServerNode;

struct ServerEntry {
    ServerEntry(angel::evloop *loop, angel::inet_addr conn_addr)
        : client(new angel::client(loop, conn_addr, true, 1000))
    {
    }
    void start(ServerNode *self);
    std::unique_ptr<angel::client> client; // 到该服务器的连接
    // 初始化为领导人上一条日志的索引值+1
    size_t next_index = 0;  // 发送给该服务器的下一条日志条目的索引
    // match_index=1表示log_entries[0]已经复制到该服务器上了
    size_t match_index = 0; // 已经复制到该服务器上的最大日志的索引+1
};

class ServerNode {
public:
    // 保存所有其他服务器，方便与之通信并维护必要的状态信息
    using ServerEntryMap = std::unordered_map<std::string, std::unique_ptr<ServerEntry>>;
    // 保存日志索引所对应的请求的客户端<log_index, conn_id>
    // 在该日志条目被领导人执行后，会根据conn_id进行回复
    using Clients = std::unordered_map<size_t, size_t>;

    ServerNode(angel::evloop *loop, angel::inet_addr listen_addr)
        : server(loop, listen_addr)
    {
        this->loop = loop;
        initServer();
        server.set_message_handler([this](const angel::connection_ptr& conn, angel::buffer& buf){
                this->process(conn, buf);
                });
        server.start();
    }

    void initServer();
    void serverCron();

    void process(const angel::connection_ptr& conn, angel::buffer& buf);
    void processRpcFromServer(const angel::connection_ptr& conn, angel::buffer& buf);
    void processrpc(const angel::connection_ptr& conn, rpc& r);
    void applyLogEntry();
    void processRpcAsLeader(const angel::connection_ptr& conn, rpc& r);
    void processRpcAsCandidate(const angel::connection_ptr& conn, rpc& r);
    void processRpcAsFollower(const angel::connection_ptr& conn, rpc& r);

    void sendLogEntry();
    void recvLogEntryFromLeader(const angel::connection_ptr& conn, AppendEntry& ae);
    void votedForCandidate(const angel::connection_ptr& conn, RequestVote& rv);

    void sendLogEntrySuccessfully(const angel::connection_ptr& conn);
    void sendLogEntryFail(const angel::connection_ptr& conn);

    void setHeartBeatTimer();
    void sendHeartBeat();
    void cancelHeartBeatTimer();

    void startLeaderElection();
    void setElectionTimer();
    void cancelElectionTimer();
    void requestServersToVote();
    void becomeNewLeader();

    // nodes = 2, half = 2
    // nodes = 3, half = 2
    // nodes = 4, half = 3
    // nodes = 5, half = 3
    size_t getHalf() { return (server_entries.size() + 1) / 2 + 1; }

    static std::string generate_runid();

    void info(const char *fmt, ...);
private:
    bool logUpToDate(size_t, size_t);
    void appendLogEntry(size_t term, const std::string& cmd);
    void updateCommitIndex(AppendEntry& ae);
    void updateLastRecvHeartbeatTime()
    {
        last_recv_heartbeat_time = angel::util::get_cur_time_ms();
    }
    // remove log_entries[from, end] and sync changes to [logfile]
    void removeLogEntry(size_t from);
    void clearCandidateInfo();
    void clearLeaderInfo();

    // 在所有服务器上持久存在的
    std::string run_id;         // 服务器的运行时id（分布式唯一id）
    size_t current_term = 0;    // 服务器的当前任期，单调递增
    std::string voted_for;      // 当前任期内收到选票的候选人id（投给了谁）
    size_t votes;               // 当前任期内收到了多少票数
    LogEntryList log_entries;   // 要执行的日志条目
    ////////////////////////////////////////////////////
    // 在所有服务器上不稳定存在的
    // 如果一条日志被复制到了大多数服务器上，就称为‘可提交的‘
    // commit_index=1表示log_entries[0]已被提交了
    size_t commit_index = 0;    // 已被提交的最大日志条目的索引
    // last_applied=1表示log_entries[0]已被应用到状态机了
    size_t last_applied = 0;    // 被状态机执行的最大日志条目的索引
    ////////////////////////////////////////////////////
    // 在领导人服务器上不稳定存在的（赢得选举之后初始化）
    ServerEntryMap server_entries;
    ////////////////////////////////////////////////////
    // 具体实现需要的一些数据
    Role role = FOLLOWER;
    angel::evloop *loop;
    angel::server server;
    // leader在接收客户端命令和回复响应时使用
    Clients clients;
    // 选举超时计时器和心跳计时器的id
    size_t election_timer_id = 0;
    size_t heartbeat_timer_id = 0;
    // 最后一次收到心跳包的时间戳(ms)
    int64_t last_recv_heartbeat_time = angel::util::get_cur_time_ms();
    int log_fd = -1;
};

}

#endif // _RAFT_SRC_RAFT_H
