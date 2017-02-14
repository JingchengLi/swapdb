//
// Created by zts on 17-1-3.
//

#ifndef SSDB_REPONSE_REDIS_H
#define SSDB_REPONSE_REDIS_H

#include <vector>
#include <string>

#define REDIS_REPLY_STRING 1
#define REDIS_REPLY_ARRAY 2
#define REDIS_REPLY_INTEGER 3
#define REDIS_REPLY_NIL 4
#define REDIS_REPLY_STATUS 5
#define REDIS_REPLY_ERROR 6

#define REDIS_RESPONSE_DONE 1
#define REDIS_RESPONSE_ERR -1
#define REDIS_RESPONSE_RETRY -2

class RedisResponse {
public:
    int status = 0;

    int type;
    long long integer;

    std::string str;
    std::vector<RedisResponse *> element;

    void reset() {
        status = 0;
        str = "";

        for (int i = 0; i < element.size(); ++i) {
            if (element[i] != NULL) {
                delete element[i];
            }
        }

        element.clear();

    }

    virtual ~RedisResponse() {
        for (int i = 0; i < element.size(); ++i) {
            if (element[i] != NULL) {
                delete element[i];
            }

        }

        element.clear();

    }

    std::string toString() {
        if (status != 1) {
            return "error status :" + status;
        }

        switch (type) {
            case REDIS_REPLY_NIL:
                return "nil";
            case REDIS_REPLY_STRING:
            case REDIS_REPLY_STATUS:
            case REDIS_REPLY_ERROR:
                return str;
            case REDIS_REPLY_INTEGER:
                return ::str((int64_t) integer);
            case REDIS_REPLY_ARRAY: {
                std::string tmp;
                for (int i = 0; i < element.size(); ++i) {
                    tmp.append("(");
                    tmp.append(::str(i));
                    tmp.append(")");
                    tmp.append(element[i]->toString());
                    tmp.append("\n");
                }
                return tmp;
            }

        }


        return "ERR ?";

    }

    bool isOk(){
        return (status == 1 && str == "OK");
    }

};

#endif //SSDB_REPONSE_REDIS_H
