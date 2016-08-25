#include <iostream>
#include <assert.h>
#include "xdebug.h"
#include <string>
#include "nemo.h"

using namespace nemo;

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "./nemock db_path type pattern" << std::endl;
  std::cout << "type is one of: kv, hash, list, zset, set, all" << std::endl;
  std::cout << "Example: " << std::endl;
  std::cout << "./nemock ./db list \\*" << std::endl;
}

void ChecknRecoverSpecify(nemo::Nemo *const db, DBType type,
    const std::string& type_name, const std::string& pattern) {
    Status s = db->CheckMetaSpecify(type, pattern);
    if (!s.ok()) {
      log_err("Check and Recover %s failed : %s", type_name.c_str(), s.ToString().c_str());
    }
    log_info("Check and Recover %s success", type_name.c_str());
}


void ChecknRecover(nemo::Nemo *const db, const std::string& type, 
    const std::string& pattern) {
  bool all = false;
  if (type == "all") {
    all = true;
  }
  if (all || type == "hash") {
    ChecknRecoverSpecify(db, kHASH_DB, type, pattern);
  } 
  if (all || type == "list") {
    ChecknRecoverSpecify(db, kLIST_DB, type, pattern);
  } 
  if (all || type == "set") {
    ChecknRecoverSpecify(db, kSET_DB, type, pattern);
  } 
  if (all || type == "zset") {
    ChecknRecoverSpecify(db, kZSET_DB, type, pattern);
  }
}


int main(int argc, char **argv)
{
  if (argc != 4) {
    Usage();
    log_err("not enough parameter");
  }
  std::string path(argv[1]);
  std::string db_type(argv[2]);
  std::string pattern(argv[3]);

  if (db_type != "hash" && db_type != "list"
      && db_type != "set" && db_type != "zset"
      && db_type != "all") {
    Usage();
    log_err("invalid type parameter");
  }
  // Create nemo handle
  nemo::Options option;
  option.write_buffer_size = 268435456;
  option.target_file_size_base = 20971520;
  log_info("Prepare DB...");
  nemo::Nemo* db = new nemo::Nemo(path, option);
  assert(db);
  log_info("Check and Recover Begin");
  ChecknRecover(db, db_type, pattern);
  delete db;
  log_info("Chekc and Recover Finshed");
  return 0;
}
