/*
Copyright (c) 2017, Timothy. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/

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
private:
    std::vector<RedisResponse *> element;



public:
    int status = 0;

    int type;
    long long integer;

    std::string str;

    RedisResponse() {}


    RedisResponse(long long int integer) : integer(integer) {
        type = REDIS_REPLY_INTEGER;
    }

    RedisResponse(const std::string &str) : str(str) {
        type = REDIS_REPLY_STRING;
    }

    RedisResponse(const char *str) : str(str) {
        type = REDIS_REPLY_STRING;
    }

    RedisResponse(const std::vector<std::string> &v) {
        type = REDIS_REPLY_ARRAY;
        for (const auto &it :v) {
            element.push_back(new RedisResponse(it));
        }
    }

    void push_back(RedisResponse *response) {
        element.push_back(response);
    }



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
            char buf[21] = {0};
            snprintf(buf, sizeof(buf), "%u", status);
            return std::string("error status :") + std::string(buf);
        }

        switch (type) {
            case REDIS_REPLY_NIL:
                return "nil";
            case REDIS_REPLY_STRING:
            case REDIS_REPLY_STATUS:
            case REDIS_REPLY_ERROR:
                return str;
            case REDIS_REPLY_INTEGER:
                return std::to_string(integer);
            case REDIS_REPLY_ARRAY: {
                std::string tmp;
                for (int i = 0; i < element.size(); ++i) {
                    tmp.append("(");
                    tmp.append(std::to_string(i));
                    tmp.append(")");
                    tmp.append(element[i]->toString());
                    tmp.append("\n");
                }
                return tmp;
            }

        }


        return "ERR ?";

    }

    bool isOk() {
        return (status == 1 && str == "OK");
    }


    std::string toRedis() {

        std::string tmp;

        switch (type) {
            case REDIS_REPLY_NIL:
                tmp += "$-1\r\n";
                break;

            case REDIS_REPLY_STRING:
                tmp += "$" + std::to_string(str.length()) + "\r\n";
                if (str.length() != 0) {
                    tmp += str + "\r\n";
                }
                break;

            case REDIS_REPLY_STATUS:
                tmp += "+" + str + "\r\n";
                break;

            case REDIS_REPLY_ERROR:
                tmp += "-" + str + "\r\n";
                break;

            case REDIS_REPLY_INTEGER:
                tmp += ":" + std::to_string(integer) + "\r\n";
                break;

            case REDIS_REPLY_ARRAY: {
                tmp += "*" + std::to_string(element.size()) + "\r\n";
                for (int i = 0; i < element.size(); ++i) {
                    tmp += element[i]->toRedis();
                }
                break;
            }

        }

        return tmp;
    }
};

#endif //SSDB_REPONSE_REDIS_H
