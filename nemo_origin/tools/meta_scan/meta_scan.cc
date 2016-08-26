#include <iostream>
#include <assert.h>
#include "xdebug.h"
#include <string>
#include "nemo.h"

using namespace nemo;

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "./meta_scan db_path type pattern" << std::endl;
  std::cout << "type is one of: kv, hash, list, zset, set, all" << std::endl;
  std::cout << "Example: " << std::endl;
  std::cout << "./meta_scan ./db list \\*" << std::endl;
}

void PrintMetaSpecify(nemo::Nemo *const db, DBType type, 
    const std::string& type_name, const std::string& pattern) {
  // Scan metas info
  std::map<std::string, MetaPtr> metas;
  Status s = db->ScanMetasSpecify(type, pattern, metas);
  if (!s.ok()) {
    log_err("Failed to scan metas");
    return;
  }
  // Print
  std::cout << "---------------- Begin Scan[" << type_name << "] ----------------" << std::endl;
  std::map<std::string, MetaPtr>::iterator it = metas.begin();
  for (; it != metas.end(); ++it) {
    std::cout << "Key[" << it->first 
      << "], Metas[" << it->second->ToString() << "]" << std::endl;
  }
  std::cout << "---------------- End Scan[" << type_name << "] ----------------" << std::endl;
}

void PrintMeta(nemo::Nemo *const db, const std::string& type, 
    const std::string& pattern) {
  bool all = false;
  if (type == "all") {
    all = true;
  }
  if (all || type == "hash") {
    PrintMetaSpecify(db, kHASH_DB, "hash", pattern);
  } 
  if (all || type == "list") {
    PrintMetaSpecify(db, kLIST_DB, "list", pattern);
  } 
  if (all || type == "set") {
    PrintMetaSpecify(db, kSET_DB, "set", pattern);
  } 
  if (all || type == "zset") {
    PrintMetaSpecify(db, kZSET_DB, "zset", pattern);
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
  log_info("SCan Begin");

  PrintMeta(db, db_type, pattern);
  delete db;

  log_info("SCan Finshed");
  return 0;
}
