#ifndef _RAFT_SRC_UTIL_H
#define _RAFT_SRC_UTIL_H

#include <sys/stat.h>
#include <unistd.h>

namespace raft {

inline off_t getFileSize(int fd)
{
    struct stat st;
    fstat(fd, &st);
    return st.st_size;
}

inline std::string getTmpFile()
{
    char tmpfile[32] = "tmp.XXXXXX";
    return mktemp(tmpfile);
}

}

#endif // _RAFT_SRC_UTIL_H
