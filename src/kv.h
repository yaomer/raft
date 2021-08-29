#ifndef _RAFT_SRC_KV_H
#define _RAFT_SRC_KV_H

#include <string>
#include <vector>
#include <unordered_map>

namespace raft {

// 一个简单的K-V服务
// 1) set key value
// 2) get key
class kv {
public:
    typedef std::string Key;
    typedef std::string Value;
    typedef std::vector<std::string> Argv;
    void execute(const std::string& cmd)
    {
        if (!parse(cmd)) {
            set_reply("-format error");
            return;
        }
        if (comp(argv[0], "set")) {
            if (argv.size() != 3) {
                set_reply("-argument number error");
            } else {
                mp[argv[1]] = argv[2];
                set_reply("+ok");
            }
        } else if (comp(argv[0], "get")) {
            if (argv.size() != 2) {
                set_reply("-argument number error");
            } else {
                set_reply(mp.count(argv[1]) ? "$" + mp[argv[1]] : "-null");
            }
        } else {
            set_reply("-unknown command");
        }
    }
    std::string get_reply()
    {
        return reply;
    }
private:
    bool comp(const std::string& s, const std::string& t)
    {
        return strcasecmp(s.c_str(), t.c_str()) == 0;
    }
    // set key hello
    // set key "hello, world"
    // 双引号不能嵌套
    bool parse(const std::string& cmd)
    {
        argv.clear();
        auto s = cmd.begin(), es = cmd.end();
        while (true) {
            s = std::find_if_not(s, es, isspace);
            if (s == es) return true;
            if (*s == '\"') {
                auto p = std::find(s + 1, es, '\"');
                if (p == es) return false;
                argv.emplace_back(std::string(s + 1, p));
                s = p + 1;
            } else {
                auto p = std::find_if(s, es, isspace);
                argv.emplace_back(std::string(s, p));
                if (p == es) return true;
                s = p + 1;
            }
        }
    }
    void set_reply(const std::string& rs)
    {
        reply = rs + "\r\n";
    }
    std::unordered_map<Key, Value> mp;
    std::string reply;
    Argv argv;
};

}

#endif // _RAFT_SRC_KV_H
