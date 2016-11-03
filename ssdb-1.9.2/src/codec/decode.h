//
// Created by a1 on 16-11-3.
//

#ifndef SSDB_DECODE_H
#define SSDB_DECODE_H

#include "util.h"

class MetaKey{
public:
    int DecodeMetaKey(const string& str);
public:
    uint16_t slot;
    string   key;
};

class ItemKey{
public:
     virtual int DecodeItemKey(const string& str);

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
    virtual int DecodeItemKey(const string& str);

public:
    double      score;
};

class ListItemKey : public ItemKey{
public:
    virtual int DecodeItemKey(const string& str);

public:
    uint64_t    seq;
};

static double decodeScore(const int64_t score);

/*
 * decode meta value class
 */
class KvMetaVal{
public:
    int DecodeMetaVal(const string& str);

public:
    char        type;
    string      value;
};

class MetaVal{
public:
    virtual int DecodeMetaVal(const string& str);

public:
    char        type;
    uint64_t    length;
    uint16_t    version;
    char        del;
};
typedef MetaVal HashMetaVal;
typedef MetaVal SetMetaVal;
typedef MetaVal ZSetMetaVal;

class ListMetaVal : public MetaVal{
public:
    virtual int DecodeMetaVal(const string& str);

public:
    uint64_t    left;
    uint64_t    right;
};

#endif //SSDB_DECODE_H
