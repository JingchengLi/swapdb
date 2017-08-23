//
// Created by zts on 17-6-14.
//

#ifndef SSDB_FILE2_H
#define SSDB_FILE2_H


#include <rocksdb/status.h>
#include <vector>


namespace R2File {

    using rocksdb::Status;

    struct DirAttributes {

        std::string abs_path;
        std::string path;
        std::vector<std::string> files;
        std::vector<DirAttributes> dirs;

    };


    Status GetChildren(const std::string &dir, DirAttributes &result, int64_t depth = INT64_MAX, std::string base = ".");

    Status GetFileSize(const std::string &fname, uint64_t *size);

}
#endif //SSDB_FILE2_H
