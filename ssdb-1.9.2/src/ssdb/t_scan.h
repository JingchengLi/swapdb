//
// Created by zts on 17-3-30.
//

#ifndef SSDB_T_SCAN_H
#define SSDB_T_SCAN_H

#include <string>

template<typename T>
bool doScanGeneric(const T &mit, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) {
    bool end = false; //scan end

    bool fulliter = (pattern == "*");
    while(mit->next()){
        if (limit == 0) {
            break; //check limit
        }

        if (fulliter || stringmatchlen(pattern.data(), pattern.length(), mit->key.data(), mit->key.length(), 0)) {
            resp.push_back(mit->key);
        } else {
            //skip
        }
        limit --;
        if (limit == 0) {
            break; //stop now
        }
    }

    if (!mit->next()) { // check iter , and update next as last key
        //scan end
        end = true;
    } else if (limit != 0) {
        //scan end
        end = true;
    }

    return end;
}

template<> inline
bool doScanGeneric(const std::unique_ptr<HIterator> &mit, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) {
    bool end = false; //scan end

    bool fulliter = (pattern == "*");
    while(mit->next()){
        if (limit == 0) {
            break; //check limit
        }

        if (fulliter || stringmatchlen(pattern.data(), pattern.length(), mit->key.data(), mit->key.length(), 0)) {
            resp.push_back(mit->key);
            resp.push_back(mit->val);
        } else {
            //skip
        }
        limit --;
        if (limit == 0) {
            break; //stop now
        }
    }

    if (!mit->next()) { // check iter , and update next as last key
        //scan end
        end = true;
    } else if (limit != 0) {
        //scan end
        end = true;
    }

    return end;
}


template<> inline
bool doScanGeneric(const std::unique_ptr<ZIterator> &mit, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) {
    bool end = false; //scan end

    bool fulliter = (pattern == "*");
    while(mit->next()){
        if (limit == 0) {
            break; //check limit
        }

        if (fulliter || stringmatchlen(pattern.data(), pattern.length(), mit->key.data(), mit->key.length(), 0)) {
            resp.push_back(mit->key);
            resp.push_back(str(mit->score));
        } else {
            //skip
        }
        limit --;
        if (limit == 0) {
            break; //stop now
        }
    }

    if (!mit->next()) { // check iter , and update next as last key
        //scan end
        end = true;
    } else if (limit != 0) {
        //scan end
        end = true;
    }

    return end;
}

#endif //SSDB_T_SCAN_H
