#include "kv.h"

int main(int argc, char *argv[])
{
    angel::set_log_level(angel::logger::level::info);
    angel::evloop loop;
    if (argc != 2) {
        printf("usage: serv [listen port]\n");
        return 1;
    }
    angel::inet_addr listen_addr("127.0.0.1", atoi(argv[1]));
    raft::ServerNode raft_node(&loop, listen_addr, "../../raft.conf", new kv());
    raft_node.start();
    loop.run();
}
