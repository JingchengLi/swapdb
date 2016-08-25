#ifndef NEMO_INCLUDE_NEMO_BACKUPABLE_H_
#define NEMO_INCLUDE_NEMO_BACKUPABLE_H_
#include <string>
#include <map>
#include <atomic>
#include "rocksdb/db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "nemo.h"
#include "nemo_const.h"

namespace nemo {
    const std::string DEFAULT_BK_PATH = "dump"; //Default backup root dir
    const std::string DEFAULT_RS_PATH = "db";   //Default restore root dir

    // Arguments which will used by BackupSave Thread
    // p_engine for BackupEngine handler
    // backup_dir
    // key_type kv, hash, list, set or zset
    struct BackupSaveArgs {
        void *p_engine;
        const std::string backup_dir;
        const std::string key_type;
        Status res;

        BackupSaveArgs(void *_p_engine, const std::string &_backup_dir,
                const std::string &_key_type)
            : p_engine(_p_engine), backup_dir(_backup_dir), key_type(_key_type) {}
    };

    struct BackupContent {
        std::vector<std::string> live_files;
        uint64_t manifest_file_size = 0;
        uint64_t sequence_number = 0;
    };

    class BackupEngine {
        public:
            ~BackupEngine();
            static Status Open(nemo::Nemo *db, BackupEngine** backup_engine_ptr);

            Status SetBackupContent();
            
            Status CreateNewBackup(const std::string &dir);
            
            void StopBackup();
    
            Status CreateNewBackupSpecify(const std::string &dir, const std::string &type);
        private:
            BackupEngine() {}

            std::map<std::string, rocksdb::Checkpoint*> engines_;
            std::map<std::string, BackupContent> backup_content_;
            std::map<std::string, pthread_t> backup_pthread_ts_;
            
            Status NewCheckpoint(rocksdb::DBWithTTL *tdb, const std::string &type);
            std::string GetSaveDirByType(const std::string _dir, const std::string& _type) const {
                std::string backup_dir = _dir.empty() ? DEFAULT_BK_PATH : _dir;
                return backup_dir + ((backup_dir.back() != '/') ? "/" : "") + _type;
            }
            Status WaitBackupPthread();
    };
}

#endif
