#include "logs.h"
#include "util.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include <angel/logger.h>
#include <angel/util.h>

namespace raft {

Logs::~Logs()
{
    close(logfd);
}

void Logs::setLogFile(const std::string& file)
{
    logfile = file;
    openLogFile();
}

void Logs::openLogFile()
{
    logfd = open(logfile.c_str(), O_RDWR | O_APPEND | O_CREAT, 0644);
    if (logfd < 0) {
        log_fatal("can't open %s: %s", logfile.c_str(), angel::util::strerrno());
    }
}

void Logs::setLastLog(size_t last_index, size_t last_term)
{
    base_index = last_index + 1;
    last_included_term = last_term;
}

size_t Logs::lastTerm()
{
    if (empty()) return 0;
    return end() == base_index ? last_included_term : operator[](end() - 1).term;
}

// [term][cmd-len][cmd]
void Logs::append(size_t term, std::string&& cmd)
{
    struct iovec iov[3];
    iov[0].iov_base = &term;
    iov[0].iov_len = sizeof(term);
    size_t len = cmd.size();
    iov[1].iov_base = &len;
    iov[1].iov_len = sizeof(len);
    iov[2].iov_base = const_cast<char*>(cmd.data());
    iov[2].iov_len = len;
    if (writev(logfd, iov, 3) < 0) {
        log_fatal("writev(logfd=%d) error: %s", logfd, angel::util::strerrno());
    }
    fsync(logfd);
    logs.emplace_back(term, std::move(cmd));
}

void Logs::append(WriteBatch& batch)
{
    if (batch.size() == 0) return;
    size_t iovlen = batch.size() * 3;
    assert(iovlen <= IOV_MAX);
    struct iovec iov[iovlen];
    size_t lenv[iovlen / 3];
    int i = 0, j = 0;
    for (auto& log : batch.wque) {
        iov[i].iov_base = &log.term;
        iov[i].iov_len = sizeof(log.term);
        lenv[j] = log.cmd.size();
        iov[i + 1].iov_base = &lenv[j];
        iov[i + 1].iov_len = sizeof(lenv[j]);
        iov[i + 2].iov_base = const_cast<char*>(log.cmd.data());
        iov[i + 2].iov_len = lenv[j];
        i += 3;
        j++;
    }
    if (writev(logfd, iov, iovlen) < 0) {
        log_fatal("writev(logfd=%d) error: %s", logfd, angel::util::strerrno());
    }
    fsync(logfd);
    for (auto& log : batch.wque) {
        logs.emplace_back(std::move(log));
    }
}

void Logs::remove(size_t from)
{
    off_t remove_bytes = 0;
    if (from < base_index) return;
    if (logs.empty()) return;
    from -= base_index;
    for (size_t i = from; i < logs.size(); i++) {
        remove_bytes += logs[i].cmd.size() + sizeof(size_t) * 2;
    }
    logs.erase(logs.begin() + from, logs.end());
    off_t filesize = getFileSize(logfd);
    off_t remain_bytes = filesize - remove_bytes;
    auto tmpfile = getTmpFile();
    int tmpfd = open(tmpfile.c_str(), O_RDWR | O_APPEND | O_CREAT, 0644);
    void *start = mmap(nullptr, remain_bytes, PROT_READ, MAP_SHARED, logfd, 0);
    if (write(tmpfd, start, remain_bytes) < 0) {
        log_fatal("write error: %s", angel::util::strerrno());
    }
    fsync(tmpfd);
    close(tmpfd);
    munmap(start, remain_bytes);
    rename(tmpfile.c_str(), logfile.c_str());
    close(logfd);
    openLogFile();
}

void Logs::removeBefore(size_t to)
{
    off_t remove_bytes = 0;
    if (to < base_index) return;
    if (logs.empty()) return;
    to -= base_index;
    for (size_t i = 0; i <= to; i++) {
        remove_bytes += logs[i].cmd.size() + sizeof(size_t) * 2;
    }
    logs.erase(logs.begin(), logs.begin() + to + 1);
    off_t filesize = getFileSize(logfd);
    auto tmpfile = getTmpFile();
    int tmpfd = open(tmpfile.c_str(), O_RDWR | O_APPEND | O_CREAT, 0644);
    char *start = reinterpret_cast<char*>(mmap(nullptr, filesize, PROT_READ, MAP_SHARED, logfd, 0));
    if (write(tmpfd, start + remove_bytes, filesize - remove_bytes) < 0) {
        log_fatal("write error: %s", angel::util::strerrno());
    }
    fsync(tmpfd);
    close(tmpfd);
    munmap(start, filesize);
    rename(tmpfile.c_str(), logfile.c_str());
    close(logfd);
    openLogFile();
}

void Logs::load()
{
    auto filesize = getFileSize(logfd);
    void *start = mmap(nullptr, filesize, PROT_READ, MAP_SHARED, logfd, 0);
    if (start == MAP_FAILED) return;
    char *buf = reinterpret_cast<char*>(start);
    char *end = buf + filesize;
    while (buf < end) {
        size_t term = *reinterpret_cast<size_t*>(buf);
        buf += sizeof(term);
        size_t len = *reinterpret_cast<size_t*>(buf);
        buf += sizeof(len);
        std::string cmd(buf, len);
        buf += len;
        logs.emplace_back(term, std::move(cmd));
    }
    munmap(start, filesize);
}

}
