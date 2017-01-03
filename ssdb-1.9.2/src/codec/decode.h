//
// Created by a1 on 16-11-3.
//

#ifndef SSDB_DECODE_H
#define SSDB_DECODE_H

#include "util.h"
class Bytes;

class MetaKey{
public:
    int DecodeMetaKey(const Bytes& str);
public:
    uint16_t slot;
    string   key;
};

class ItemKey{
public:
     virtual int DecodeItemKey(const Bytes& str);

public:
    uint16_t    version;
    string      key;
    string      field;
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
    int DecodeMetaVal(const Bytes& str);

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
    char        del;
    uint16_t    version;
    uint64_t    length;
};
typedef MetaVal HashMetaVal;
typedef MetaVal SetMetaVal;
typedef MetaVal ZSetMetaVal;

class ListMetaVal : public MetaVal{
public:
    virtual int DecodeMetaVal(const Bytes& str);

public:
    uint64_t    left_seq;
    uint64_t    right_seq;
};

/*
 * decode delete key class
 */
class DeleteKey{
public:
    int DecodeDeleteKey(const Bytes& str);

public:
    char        type;
    uint16_t    slot;
    uint16_t    version;
    string      key;
};

class BQueueKey{
public:
    int DecodeBQueueKey(const string& str);

public:
    uint16_t type;
    string   key;
};


#define POS_TYPE 0
#define POS_DEL  3


#endif //SSDB_DECODE_H
