//
// Created by zts on 17-4-27.
//
#include "replication.h"
#include <util/thread.h>
#include <net/link.h>
#include <util/cfree.h>
#include "serv.h"
#include "rocksdb/utilities/backupable_db.h"

extern "C" {
#include <redis/rdb.h>
#include <redis/zmalloc.h>
#include <redis/lzf.h>
};

int ReplicationByIterator3::process() {
    log_info("ReplicationByIterator3::process");


    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    Link *master_link = upstream;

    rocksdb::Status s;
    rocksdb::DB *db = serv->ssdb->getLdb();

    auto backup_option = rocksdb::BackupableDBOptions(serv->ssdb->getData_path() + "/../backup");
//    backup_option.sync = true;
    backup_option.share_table_files = true;
    backup_option.share_files_with_checksum = true;

    rocksdb::BackupEngine *backup_engine = nullptr;
    s = rocksdb::BackupEngine::Open(rocksdb::Env::Default(), backup_option, &backup_engine);
    assert(s.ok());

    PTST(ReplicationByIterator3, 1)
    s = backup_engine->CreateNewBackup(db, true);
    assert(s.ok());
    PTE(ReplicationByIterator3 , "CreateNewBackup")

    backup_engine->PurgeOldBackups(1);
    PTE(ReplicationByIterator3 , "PurgeOldBackups")


    std::vector<rocksdb::BackupInfo> backup_infos;
    backup_engine->GetBackupInfo(&backup_infos);

    for (auto const &backup_info : backup_infos) {
        log_info("backup_info: ID:%d TS:%d Size:%d ", backup_info.backup_id , backup_info.timestamp, backup_info.size);
//        s = backup_engine->VerifyBackup(backup_info.backup_id /* ID */);
//        assert(s.ok());
    }








    delete backup_engine;

    return 0;
}