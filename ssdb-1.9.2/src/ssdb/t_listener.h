//
// Created by zts on 17-3-8.
//

#ifndef SSDB_MYEVENTLISTENER_H
#define SSDB_MYEVENTLISTENER_H

#ifdef USE_LEVELDB
#else

#include "rocksdb/listener.h"

class t_listener : public rocksdb::EventListener {

public:
    // A call-back function to RocksDB which will be called whenever a
    // registered RocksDB flushes a file.  The default implementation is
    // no-op.
    //
    // Note that the this function must be implemented in a way such that
    // it should not run for an extended period of time before the function
    // returns.  Otherwise, RocksDB may be blocked.
    void OnFlushCompleted(rocksdb::DB* db,
                                  const rocksdb::FlushJobInfo& flush_job_info) {

        log_info("OnFlushCompleted %s ", flush_job_info.file_path.c_str());
    }

    // A call-back function for RocksDB which will be called whenever
    // a SST file is deleted.  Different from OnCompactionCompleted and
    // OnFlushCompleted, this call-back is designed for external logging
    // service and thus only provide string parameters instead
    // of a pointer to DB.  Applications that build logic basic based
    // on file creations and deletions is suggested to implement
    // OnFlushCompleted and OnCompactionCompleted.
    //
    // Note that if applications would like to use the passed reference
    // outside this function call, they should make copies from the
    // returned value.
    void OnTableFileDeleted(const rocksdb::TableFileDeletionInfo& info) {

//        log_info("OnTableFileDeleted %s" , info.file_path.c_str() );


    }

    // A call-back function for RocksDB which will be called whenever
    // a registered RocksDB compacts a file. The default implementation
    // is a no-op.
    //
    // Note that this function must be implemented in a way such that
    // it should not run for an extended period of time before the function
    // returns. Otherwise, RocksDB may be blocked.
    //
    // @param db a pointer to the rocksdb instance which just compacted
    //   a file.
    // @param ci a reference to a CompactionJobInfo struct. 'ci' is released
    //  after this function is returned, and must be copied if it is needed
    //  outside of this function.
    void OnCompactionCompleted(rocksdb::DB* db,
                                       const rocksdb::CompactionJobInfo& ci) {

        log_info("OnCompactionCompleted costs %d ms", ci.stats.elapsed_micros/1000);

    }

    // A call-back function for RocksDB which will be called whenever
    // a SST file is created.  Different from OnCompactionCompleted and
    // OnFlushCompleted, this call-back is designed for external logging
    // service and thus only provide string parameters instead
    // of a pointer to DB.  Applications that build logic basic based
    // on file creations and deletions is suggested to implement
    // OnFlushCompleted and OnCompactionCompleted.
    //
    // Historically it will only be called if the file is successfully created.
    // Now it will also be called on failure case. User can check info.status
    // to see if it succeeded or not.
    //
    // Note that if applications would like to use the passed reference
    // outside this function call, they should make copies from these
    // returned value.
    void OnTableFileCreated(const rocksdb::TableFileCreationInfo& info) {
//        log_info("OnTableFileCreated");

    }

    // A call-back function for RocksDB which will be called before
    // a SST file is being created. It will follow by OnTableFileCreated after
    // the creation finishes.
    //
    // Note that if applications would like to use the passed reference
    // outside this function call, they should make copies from these
    // returned value.
    void OnTableFileCreationStarted(
            const rocksdb::TableFileCreationBriefInfo& info) {
//        log_info("OnTableFileCreationStarted");

    }

    // A call-back function for RocksDB which will be called before
    // a memtable is made immutable.
    //
    // Note that the this function must be implemented in a way such that
    // it should not run for an extended period of time before the function
    // returns.  Otherwise, RocksDB may be blocked.
    //
    // Note that if applications would like to use the passed reference
    // outside this function call, they should make copies from these
    // returned value.
    void OnMemTableSealed(
            const rocksdb::MemTableInfo& info) {
        log_info("OnMemTableSealed, del:%d num:%d", info.num_deletes, info.num_entries);

    }


};

#endif

#endif //SSDB_MYEVENTLISTENER_H
