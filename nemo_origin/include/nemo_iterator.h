#ifndef NEMO_INCLUDE_NEMO_ITERATOR_H_
#define NEMO_INCLUDE_NEMO_ITERATOR_H_
#include "rocksdb/db.h"
#include "nemo_const.h"

namespace nemo {

enum Direction {
    kForward = 0,
    kBackward = 1
};

struct IteratorOptions {
  std::string end;
  uint64_t limit;
  rocksdb::ReadOptions read_options;
  Direction direction;

  IteratorOptions()
      : limit(1LL << 60), direction(kForward) {}
  IteratorOptions(const std::string &_end, uint64_t _limit,
                  rocksdb::ReadOptions roptions, Direction _dir = kForward)
      : end(_end), limit(_limit), read_options(roptions), direction(_dir) {}
};

class Iterator {
public:
    Iterator(rocksdb::Iterator *it, const IteratorOptions& iter_options);
    virtual ~Iterator() {
      delete it_;
    }

    rocksdb::Slice key();
    rocksdb::Slice value();
    virtual void Skip(int64_t offset);
    virtual void Next();
    virtual bool Valid();
    virtual rocksdb::ReadOptions read_options() { return ioptions_.read_options; }
    int direction()                 {  return ioptions_.direction; }

protected:
    bool valid_;

private:
    bool Check();
    
    rocksdb::Iterator *it_;
    IteratorOptions ioptions_;

    //No Copying Allowed
    Iterator(Iterator&);
    void operator=(Iterator&);
};

class KIterator : public Iterator {
public:
    KIterator(rocksdb::Iterator *it, const IteratorOptions iter_options);
    virtual void Next();
    virtual void Skip(int64_t offset);
    virtual bool Valid();
    std::string key()       { return key_; };
    std::string value()     { return value_; };
    
private:
    void CheckAndLoadData();

    std::string key_;
    std::string value_;

    //No Copying Allowed
    KIterator(KIterator&);
    void operator=(KIterator&);
};

class HIterator : public Iterator {
public:
    HIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key);
    virtual void Next();
    virtual void Skip(int64_t offset);
    virtual bool Valid();
    std::string key()   { return key_; };
    std::string field() { return field_; };
    std::string value() { return value_; };

private:
    void CheckAndLoadData();

    std::string key_;
    std::string field_;
    std::string value_;

    //No Copying Allowed
    HIterator(HIterator&);
    void operator=(HIterator&);
};

class ZIterator : public Iterator {
public:
    ZIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key);
    virtual bool Valid();
    virtual void Skip(int64_t offset);
    virtual void Next();
    std::string key()    { return key_; };
    double score()       { return score_; };
    std::string member() { return member_; };

private:
    void CheckAndLoadData();

    std::string key_;
    double score_;
    std::string member_;

    //No Copying Allowed
    ZIterator(ZIterator&);
    void operator=(ZIterator&);
    
};

class ZLexIterator : public Iterator {
public:
    ZLexIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key);
    virtual bool Valid();
    virtual void Skip(int64_t offset);
    virtual void Next();
    std::string key()       { return key_; };
    std::string member()    { return member_; };

private:
    void CheckAndLoadData();

    std::string key_;
    std::string member_;

    //No Copying Allowed
    ZLexIterator(ZLexIterator&);
    void operator=(ZLexIterator&);
};

class SIterator : public Iterator {
public:
    SIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key);
    virtual void Skip(int64_t offset);
    virtual void Next();
    virtual bool Valid();
    std::string key()       { return key_; };
    std::string member()    { return member_; };

private:
    void CheckAndLoadData();

    std::string key_;
    std::string member_;

    //No Copying Allowed
    SIterator(SIterator&);
    void operator=(SIterator&);
};

}
#endif
