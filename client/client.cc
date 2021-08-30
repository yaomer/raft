#include <angel/evloop_thread.h>
#include <angel/client.h>

#include <iostream>
#include <unordered_set>

std::vector<std::string> cluster_nodes = {
    "127.0.0.1:8000",
    "127.0.0.1:8001",
    "127.0.0.1:8002",
    "127.0.0.1:8003",
    "127.0.0.1:8004",
};

std::unordered_set<int> try_server_set;

std::string rand_select_server()
{
    int where = 0;
    srand(time(nullptr));
    do {
        where = rand() % cluster_nodes.size();
    } while (try_server_set.count(where));
    try_server_set.insert(where);
    if (try_server_set.size() == cluster_nodes.size()) {
        try_server_set.clear();
    }
    return cluster_nodes[where];
}

int connect_timeout = 1000;

bool is_leader_collapse = true;

class client {
public:
    client() : cli(t_loop.wait_loop(), angel::inet_addr(rand_select_server()), false, 300)
    {
        cli.set_connection_handler([](const angel::connection_ptr& conn){
                std::cout << "### connect with server " << conn->get_peer_addr().to_host() << " \n";
                is_leader_collapse = true;
                try_server_set.clear();
                });
        cli.set_message_handler([this](const angel::connection_ptr& conn, angel::buffer& buf){
                if (buf.starts_with("<host>")) {
                    std::string host(buf.peek() + 6, buf.peek() + buf.find_crlf());
                    if (host != "") {
                        std::cout << "### " << conn->get_peer_addr().to_host() << " is not a leader, "
                                  << "redirect to leader " << host << "\n";
                        this->reconnect(host);
                    } else {
                        std::cout << "### the cluster currently has no leader\n";
                    }
                    return;
                }
                std::cout << "(reply) " << buf.c_str();
                buf.retrieve_all();
                });
        cli.set_close_handler([this](const angel::connection_ptr& conn){
                if (!is_leader_collapse) return;
                auto host = rand_select_server();
                std::cout << "### disconnect with server " << conn->get_peer_addr().to_host()
                          << ", try to connect " << host << "\n";
                this->reconnect(host);
                });
        cli.not_exit_loop();
    }
    void start()
    {
        cli.start();
        set_connect_timeout_timer();
    }
    bool is_connected()
    {
        return cli.is_connected();
    }
    const angel::connection_ptr& conn()
    {
        return cli.conn();
    }
private:
    void connect_timeout_handler()
    {
        if (!cli.is_connected()) {
            auto host = rand_select_server();
            std::cout << "### connect " << cli.get_peer_addr().to_host()
                      << " timeout, try to connect " << host << "\n";
            is_leader_collapse = false;
            cli.restart(angel::inet_addr(host));
            set_connect_timeout_timer();
        }
    }

    void reconnect(const std::string& host)
    {
        is_leader_collapse = false;
        cli.restart(angel::inet_addr(host));
        set_connect_timeout_timer();
    }

    void set_connect_timeout_timer()
    {
        t_loop.get_loop()->run_after(connect_timeout, [this]{ this->connect_timeout_handler(); });
    }

    angel::evloop_thread t_loop;
    angel::client cli;
};

int main(int argc, char *argv[])
{
    client cli;
    cli.start();
    // 主线程读取标准输入
    char buf[1024];
    std::string prefix("<user>");
    while (fgets(buf, sizeof(buf), stdin)) {
        buf[strlen(buf) - 1] = '\0';
        if (cli.is_connected()) {
            cli.conn()->send(prefix + buf + "\r\n");
        } else {
            std::cout << "connecting ...\n";
        }
    }
}
