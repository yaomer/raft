#ifndef _RAFT_SRC_KV_H
#define _RAFT_SRC_KV_H

#include <string>
#include <vector>
#include <unordered_map>

#include <raft/raft.h>

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>

// 一个简单的K-V服务
// 1) set key value
// 2) get key
class kv : public raft::Service {
public:
    typedef std::string Key;
    typedef std::string Value;
    typedef std::vector<std::string> Argv;
    kv() { name = "kv-server"; }
    void apply(const std::string& cmd) override
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
    std::string reply() override
    {
        return res;
    }
    void saveSnapshot(const std::string& filename) override
    {
        int fd = open(filename.c_str(), O_RDWR | O_APPEND);
        child_pid = fork();
        if (child_pid == 0) {
            struct iovec iov[4];
            for (const auto& [k, v] : mp) {
                // [key-len][key][value-len][value]
                size_t klen = k.size();
                iov[0].iov_base = &klen;
                iov[0].iov_len = sizeof(klen);
                iov[1].iov_base = const_cast<char*>(k.data());
                iov[1].iov_len = klen;
                size_t vlen = v.size();
                iov[2].iov_base = &vlen;
                iov[2].iov_len = sizeof(vlen);
                iov[3].iov_base = const_cast<char*>(v.data());
                iov[3].iov_len = vlen;
                writev(fd, iov, 4);
            }
            fsync(fd);
            close(fd);
            exit(0);
        }
    }
    bool isSavedSnapshot() override
    {
        if (child_pid != -1) {
            if (waitpid(child_pid, nullptr, WNOHANG) > 0) {
                child_pid = -1;
                return true;
            }
        }
        return false;
    }
    void loadSnapshot(const std::string& filename, off_t offset) override
    {
        int fd = open(filename.c_str(), O_RDONLY);
        struct stat st;
        fstat(fd, &st);
        void *start = mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
        if (start == MAP_FAILED) return;
        char *buf = reinterpret_cast<char*>(start);
        char *end = buf + st.st_size;
        buf += offset;
        while (buf < end) {
            size_t len = *reinterpret_cast<size_t*>(buf);
            buf += sizeof(len);
            std::string key(buf, len);
            buf += len;
            len = *reinterpret_cast<size_t*>(buf);
            buf += sizeof(len);
            std::string value(buf, len);
            buf += len;
            mp.emplace(std::move(key), std::move(value));
        }
        munmap(start, st.st_size);
        close(fd);
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
        res = rs + "\r\n";
    }
    std::unordered_map<Key, Value> mp;
    std::string res;
    Argv argv;
    pid_t child_pid = -1;
};

#endif // _RAFT_SRC_KV_H
