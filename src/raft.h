#ifndef _RAFT_SRC_RAFT_H
#define _RAFT_SRC_RAFT_H

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

#include <angel/server.h>
#include <angel/client.h>

#include "rpc.h"
#include "logs.h"
#include "service.h"

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
    // match_index=1表示logs[0]已经复制到该服务器上了
    size_t match_index = 0; // 已经复制到该服务器上的最大日志的索引+1
};

class ServerNode {
public:
    // 保存所有其他服务器，方便与之通信并维护必要的状态信息
    using ServerEntryMap = std::unordered_map<std::string, std::unique_ptr<ServerEntry>>;
    // 保存日志索引所对应的请求的客户端<log_index, conn_id>
    // 在该日志条目被领导人执行后，会根据conn_id进行回复
    using Clients = std::unordered_map<size_t, size_t>;

    ServerNode(angel::evloop *loop, angel::inet_addr listen_addr, const std::string& confile,
            Service *service = new Service())
        : loop(loop), server(loop, listen_addr), service(service)
    {
        initServer(confile);
        server.set_message_handler([this](const angel::connection_ptr& conn, angel::buffer& buf){
                this->process(conn, buf);
                });
    }
    ~ServerNode() { saveState(); }
    void start() { server.start(); }
    void info(const char *fmt, ...);
private:
    void initServer(const std::string& confile);
    void serverCron();

    ServerEntry *getServerEntry(const std::string& host)
    {
        auto it = server_entries.find(host);
        return it != server_entries.end() ? it->second.get() : nullptr;
    }

    void process(const angel::connection_ptr& conn, angel::buffer& buf);
    void processRpcFromServer(const angel::connection_ptr& conn, angel::buffer& buf);
    void processrpc(const angel::connection_ptr& conn, rpc& r);
    void applyLogEntry();
    void processRpcAsLeader(const angel::connection_ptr& conn, rpc& r);
    void processRpcAsCandidate(const angel::connection_ptr& conn, rpc& r);
    void processRpcAsFollower(const angel::connection_ptr& conn, rpc& r);

    void sendLogEntry();
    void recvLogEntry(const angel::connection_ptr& conn, AppendEntry& ae);
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

    void recvSnapshot(const angel::connection_ptr& conn, InstallSnapshot& snapshot);
    void sendSnapshot(const angel::connection_ptr& conn);
    void saveSnapshot();
    void loadSnapshot();

    bool logUpToDate(RequestVote& rv);
    void sendLogEntry(ServerEntry *serv);
    void updateCommitIndex(AppendEntry& ae);
    void updateLastRecvHeartbeatTime()
    {
        last_recv_heartbeat_time = angel::util::get_cur_time_ms();
    }

    void clearCandidateInfo();
    void clearLeaderInfo();
    void clearFollowerInfo();

    void saveState();
    void loadState();

    void setPaths();
    void connectNodes();

    std::string generateRunid();

    size_t getMajority() { return (server_entries.size() + 1) / 2 + 1; }

    void updateRecentLeader(const std::string& leader_id);

    void sendReply(const angel::connection_ptr& conn, int type, bool success)
    {
        switch (type) {
        case AE_REPLY:
            conn->format_send("AE_REPLY,%zu,%d\r\n", current_term, success);
            break;
        case RV_REPLY:
            conn->format_send("RV_REPLY,%zu,%d\r\n", current_term, success);
            break;
        case IS_REPLY:
            conn->format_send("IS_REPLY,%zu,%d\r\n", current_term, success);
            break;
        }
    }

    // 在所有服务器上持久存在的
    std::string run_id;         // 服务器的运行时id
    size_t current_term = 0;    // 服务器的当前任期，单调递增
    std::string voted_for;      // 当前任期内收到选票的候选人id（投给了谁）
    size_t votes;               // 当前任期内收到了多少票数
    Logs logs;                  // 要执行的日志条目
    ////////////////////////////////////////////////////
    // 在所有服务器上不稳定存在的
    // 如果一条日志被复制到了大多数服务器上，就称为‘可提交的‘
    // commit_index=1表示logs[0]已被提交了
    size_t commit_index = 0;    // 已被提交的最大日志条目的索引
    // last_applied=1表示logs[0]已被应用到状态机了
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
    // 用于为客户端重定向到领导人
    std::string recent_leader;
    // 生成快照和接收快照时使用的临时文件
    std::string snapshot_tmpfile;
    int snapshot_fd = -1;
    // 上一次快照包含的元信息
    size_t last_included_index = 0;
    size_t last_included_term = 0;
    // 不能同时接收和生成快照
    enum { RECV_SNAPSHOT = 1, SAVE_SNAPSHOT };
    int snapshot_state = 0;
    ////////////////////////////////////////////////////
    std::unique_ptr<Service> service;

    friend ServerEntry;
};

}

#endif // _RAFT_SRC_RAFT_H
