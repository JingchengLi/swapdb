//
// Created by zts on 17-4-27.
//
#include "replication.h"
#include <util/thread.h>
#include <net/link.h>
#include <util/cfree.h>
#include <util/file2.h>
#include "serv.h"
#include "rocksdb/utilities/backupable_db.h"

extern "C" {
#include <redis/rdb.h>
#include <redis/zmalloc.h>
#include <redis/lzf.h>
};


std::function<void(const R2mFile::DirAttributes &)> visitAllDirectory;


int ReplicationByIterator3::process() {
    log_info("ReplicationByIterator3::process");


    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    rocksdb::Status s;
    rocksdb::DB *db = serv->ssdb->getLdb();

    auto backup_option = rocksdb::BackupableDBOptions(serv->ssdb->getPath() + "/backup");
//    backup_option.sync = true;
    backup_option.share_table_files = false;
    backup_option.share_files_with_checksum = false;
    backup_option.destroy_old_data = true;

    rocksdb::BackupEngine *backup_engine = nullptr;
    s = rocksdb::BackupEngine::Open(rocksdb::Env::Default(), backup_option, &backup_engine);
    assert(s.ok());

    PTST(ReplicationByIterator3, 0.01)
//    backup_engine->PurgeOldBackups(0);

    PTE(ReplicationByIterator3, "PurgeOldBackups")

    s = backup_engine->CreateNewBackup(db, true);
    if (!s.ok()) {
        log_error("%s", s.ToString().c_str());
    }


    PTE(ReplicationByIterator3, "CreateNewBackup")

//    backup_engine->PurgeOldBackups(1);
    PTE(ReplicationByIterator3, "PurgeOldBackups")


    std::vector<rocksdb::BackupInfo> backup_infos;
    backup_engine->GetBackupInfo(&backup_infos);

    for (auto const &backup_info : backup_infos) {
        log_info("backup_info: ID:%d TS:%d Size:%d ", backup_info.backup_id, backup_info.timestamp, backup_info.size);
//        s = backup_engine->VerifyBackup(backup_info.backup_id /* ID */);
//        assert(s.ok());
    }

    leveldb::Env *env = leveldb::Env::Default();

    std::vector<leveldb::Env::FileAttributes> files;

    std::string full_path;
    env->GetAbsolutePath(serv->ssdb->getPath() + "/backup", &full_path);


    R2mFile::DirAttributes di;
    R2mFile::GetChildren(full_path, di);

    visitAllDirectory = [](const R2mFile::DirAttributes &dir) {

        for (auto const &item : dir.files) {
//            log_info("files:%s/%s ", dir.path.c_str(), item.c_str());
        }


        for (auto const &d : dir.dirs) {
//            log_info("dirs:%s ", d.path.c_str());

            visitAllDirectory(d);
        }
    };

    visitAllDirectory(di);


    s = backup_engine->RestoreDBFromLatestBackup(serv->ssdb->getPath() + "/data2",serv->ssdb->getPath() + "/data2");
    if (!s.ok()) {
        log_error("%s", s.ToString().c_str());
    }



    PTE(ReplicationByIterator3, "RestoreDBFromLatestBackup")


    delete backup_engine;

    return 0;
}