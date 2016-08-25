#include <sys/types.h>
#include <dirent.h>
#include <string>
#include <vector>
#include <utility>

#include "nemo_backupable.h"
#include "xdebug.h"
#include "util.h"

namespace nemo {

BackupEngine::~BackupEngine() {
  // Wait all children threads
  StopBackup();
  WaitBackupPthread();
  // Delete engines
  for (auto& engine : engines_) {
    delete engine.second;
  }
  engines_.clear();
}

Status BackupEngine::NewCheckpoint(rocksdb::DBWithTTL *tdb, const std::string &type) {
  rocksdb::Checkpoint* checkpoint;
  Status s = rocksdb::Checkpoint::Create(tdb, &checkpoint);
  if (!s.ok()) {
    log_warn("create checkpoint failed, error %s", s.ToString().c_str());
    return s;
  }
  engines_.insert(std::make_pair(type, checkpoint));
  return s;
}

Status BackupEngine::Open(nemo::Nemo *db,
    BackupEngine** backup_engine_ptr) { 
  *backup_engine_ptr = new BackupEngine();
  if (!*backup_engine_ptr){
    return Status::Corruption("New BackupEngine failed!");
  }

  // Create BackupEngine for each db type
  rocksdb::Status s;
  rocksdb::DBWithTTL *tdb;
  std::string types[] = {KV_DB, HASH_DB, LIST_DB, SET_DB, ZSET_DB};
  for (auto& type : types) {
    if ((tdb = db->GetDBByType(type)) == NULL) {
      s = Status::Corruption("Error db type");
    }

    if (s.ok()) {
      s = (*backup_engine_ptr)->NewCheckpoint(tdb, type);
    }

    if (!s.ok()) {
      delete *backup_engine_ptr;
      break;
    }
  }
  return s;
}

Status BackupEngine::SetBackupContent() {
  Status s;
  for (auto& engine : engines_) {
    //Get backup content
    BackupContent bcontent;
    s = engine.second->GetCheckpointFiles(bcontent.live_files,
        bcontent.manifest_file_size, bcontent.sequence_number);
    if (!s.ok()) {
      log_warn("get backup files faild for type: %s", engine.first.c_str());
      return s;
    }
    backup_content_[engine.first] = bcontent;
  }
  return s;
}

Status BackupEngine::CreateNewBackupSpecify(const std::string &backup_dir, const std::string &type) {
  std::map<std::string, rocksdb::Checkpoint*>::iterator it_engine = engines_.find(type);
  std::map<std::string, BackupContent>::iterator it_content = backup_content_.find(type);
  std::string dir = GetSaveDirByType(backup_dir, type);
  delete_dir(dir.c_str());

  if (it_content != backup_content_.end() && 
      it_engine != engines_.end()) {
    Status s = it_engine->second->CreateCheckpointWithFiles(
        dir,
        it_content->second.live_files, 
        it_content->second.manifest_file_size,
        it_content->second.sequence_number);
    if (!s.ok()) {
      log_warn("backup engine create new failed, type: %s, error %s", 
          type.c_str(), s.ToString().c_str());
      return s;
    }

  } else {
    log_warn("invalid db type: %s", type.c_str());
    return Status::Corruption("invalid db type");
  }
  return Status::OK();
}

void* ThreadFuncSaveSpecify(void *arg) {
  BackupSaveArgs* arg_ptr = static_cast<BackupSaveArgs*>(arg);
  BackupEngine* p = static_cast<BackupEngine*>(arg_ptr->p_engine);

  arg_ptr->res = p->CreateNewBackupSpecify(arg_ptr->backup_dir, arg_ptr->key_type);

  pthread_exit(&(arg_ptr->res));
}

Status BackupEngine::WaitBackupPthread() { 
  int ret;
  Status s = Status::OK();
  for (auto& pthread : backup_pthread_ts_) {
    void *res;
    if ((ret = pthread_join(pthread.second, &res)) != 0) {
      log_warn("pthread_join failed with backup thread for key_type: %s, error %d", 
          pthread.first.c_str(), ret);
    }
    Status cur_s = *(static_cast<Status*>(res));
    if (!cur_s.ok()) {
      log_warn("pthread executed failed with key_type: %s, error %s", 
          pthread.first.c_str(), cur_s.ToString().c_str());
      StopBackup(); //stop others when someone failed
      s = cur_s;
    }
  }
  backup_pthread_ts_.clear();
  return s;
}


Status BackupEngine::CreateNewBackup(const std::string &dir) {
  Status s = Status::OK();
  std::vector<BackupSaveArgs*> args;
  for (auto& engine : engines_) {
    pthread_t tid;
    BackupSaveArgs *arg = new BackupSaveArgs((void*)this, dir, engine.first);
    args.push_back(arg);
    if (pthread_create(&tid, NULL, &ThreadFuncSaveSpecify, arg) != 0) {
      s = Status::Corruption("pthead_create failed.");
      break;
    }
    if (!(backup_pthread_ts_.insert(std::make_pair(engine.first, tid)).second)) {
      log_warn("thread open dupilicated, type: %s", engine.first.c_str());
      backup_pthread_ts_[engine.first] = tid;
    }
  }

  // Wait threads stop
  if (!s.ok()) {
    StopBackup();
  }
  s = WaitBackupPthread();

  for (auto& a : args) {
    delete a;
  }
  return s;
}

void BackupEngine::StopBackup() {
  for (auto& engine : engines_) {
    if (engine.second != NULL)
      engine.second->StopCreate();  
  }
}

}
