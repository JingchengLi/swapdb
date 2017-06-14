//
// Created by zts on 17-6-14.
//

#include <vector>
#include <dirent.h>
#include <rocksdb/status.h>
#include <sys/stat.h>
#include "file2.h"

namespace R2mFile {

    using rocksdb::Status;


    Status GetChildren(const std::string &dir, DirAttributes &result, int64_t depth, std::string base) {
        result.abs_path = dir;
        result.path = base;
        errno = 0;
        DIR *d = opendir(dir.c_str());
        if (d == nullptr) {
            switch (errno) {
                case EACCES:
                case ENOENT:
                case ENOTDIR:
                    return Status::NotFound();
                default:
                    return rocksdb::Status::IOError(dir, strerror(errno));
            }
        }
        struct dirent *entry;
        while ((entry = readdir(d)) != nullptr) {

            if ((entry->d_type) & DT_DIR) {
                if (depth == 0) {
                    continue;
                }
                if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                    continue;
                DirAttributes tmp_result;
                auto tmp_res = GetChildren(dir + "/" + entry->d_name, tmp_result, depth - 1, base + "/" + entry->d_name);

                result.dirs.emplace_back(tmp_result);
            } else {
                result.files.emplace_back(entry->d_name);
            }
        }
        closedir(d);
        return Status::OK();
    }


    Status GetFileSize(const std::string &fname, uint64_t *size) {
        Status s;
        struct stat sbuf;
        if (stat(fname.c_str(), &sbuf) != 0) {
            *size = 0;
            s = rocksdb::Status::IOError(fname, strerror(errno));
        } else {
            *size = sbuf.st_size;
        }
        return s;
    }

}