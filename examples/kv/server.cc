#include "kv.h"

int main(int argc, char *argv[])
{
    angel::set_log_level(angel::logger::level::info);
    angel::evloop loop;
    if (argc != 2) {
        printf("usage: serv [confile]\n");
        return 1;
    }
    raft::ServerNode raft_node(&loop, argv[1], new kv());
    raft_node.start();
    loop.run();
}
