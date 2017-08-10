/*
Copyright (c) 2017, Timothy. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_DECODE_H
#define SSDB_DECODE_H

#include "util.h"
#include "util/bytes.h"

class MetaKey{
public:
    int DecodeMetaKey(const Bytes& str);
public:
//    uint16_t slot;
    Bytes   key;
};

class ItemKey{
public:
     virtual int DecodeItemKey(const Bytes& str);

public:
    uint16_t    version;
    string      key;
    Bytes       field;
};
typedef ItemKey HashItemKey;
typedef ItemKey SetItemKey;
typedef ItemKey ZSetItemKey;

class ZScoreItemKey : public ItemKey{
public:
    virtual int DecodeItemKey(const Bytes& str);

public:
    double      score;
};

class ListItemKey : public ItemKey{
public:
    virtual int DecodeItemKey(const Bytes& str);

public:
    uint64_t    seq;
};

class EScoreItemKey : public ItemKey{
public:
    virtual int DecodeItemKey(const Bytes& str);

public:
    int64_t     score;
};

/*
 * decode meta value class
 */
class KvMetaVal{
public:
    int DecodeMetaVal(const std::string &str, bool skip_val = false);

public:
    char        type;
    char        del;
    uint16_t    version;
    string      value;
};

class MetaVal{
public:
    virtual int DecodeMetaVal(const Bytes& str);

public:
    char        type;
    char        del = KEY_ENABLED_MASK;
    uint16_t    version = 0;
    uint64_t    length = 0;
};
typedef MetaVal HashMetaVal;
typedef MetaVal SetMetaVal;
typedef MetaVal ZSetMetaVal;

class ListMetaVal : public MetaVal{
public:
    virtual int DecodeMetaVal(const Bytes& str);

public:
    uint64_t    left_seq = 0;
    uint64_t    right_seq = UINT64_MAX;
};

/*
 * decode delete key class
 */
class DeleteKey{
public:
    int DecodeDeleteKey(const Bytes& str);

public:
    char        type;
//    uint16_t    slot;
    uint16_t    version;
    string      key;
};


double decodeScore(const int64_t score);



/*
 * decode delete key class
 */
class RepoKey{
public:
        int DecodeRepoKey(const Bytes& str);

public:
    char        type;
    uint64_t    id;
    uint64_t    timestamp;
};

#endif //SSDB_DECODE_H
