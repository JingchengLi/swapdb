#include <iostream>
#include <assert.h>
#include "xdebug.h"
#include <string>
#include "nemo.h"
#include "nemo_const.h"

using namespace std;

void Usage() {
  cout << "Usage: " << endl;
  cout << "./compact db_path type" << endl;
  cout << "type is one of: kv, hash, list, zset, set, all" << endl;
}

int main(int argc, char **argv)
{
  if (argc != 3) {
    Usage();
    log_err("not enough parameter");
  }
  std::string path(argv[1]);
  std::string db_type(argv[2]);

  // Create nemo handle
  nemo::Options option;
  option.write_buffer_size = 268435456;
  option.target_file_size_base = 20971520;

  log_info("Prepare DB...");
  nemo::Nemo* db = new nemo::Nemo(path, option);
  assert(db);
  log_info("Compact Begin");

  nemo::Status s;
  if (db_type == "kv") {
    s = db->Compact(nemo::kKV_DB, true);
  } else if (db_type == "hash") {
    s = db->Compact(nemo::kHASH_DB, true);
  } else if (db_type == "list") {
    s = db->Compact(nemo::kLIST_DB, true);
  } else if (db_type == "set") {
    s = db->Compact(nemo::kSET_DB, true);
  } else if (db_type == "zset") {
    s = db->Compact(nemo::kZSET_DB, true);
  } else if (db_type == "all") {
    s = db->Compact(nemo::kALL, true);
  } else {
    Usage();
    log_err("Error db type : %s", db_type.c_str());
  }
 // if (db_type == "all") {
 //   s = db->Compact();
 // } else {
 //   s = db->CompactSpecify(db_type);
 // }
  delete db;

  if (!s.ok()) {
    Usage();
    log_err("Compact Failed : %s", s.ToString().c_str());
  } else {
    log_info("Compact Finshed");
  }
  return 0;
}
