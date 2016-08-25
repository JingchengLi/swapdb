#include <iostream>
#include <vector>
#include <string>
#include <ctime>
#include <inttypes.h>
#include <limits>

#include <pthread.h>
#include <sys/time.h>

#include "nemo.h"
#include "xdebug.h"

using namespace nemo;
using namespace std;


char kv_key_header[] = "godblesspika_kv_key_";
char kv_value_header[] = "godblesspika_kv_value_";

char hash_key_header[] = "godblesspika_hash_key_";
char hash_field_header[] = "godblesspika_hash_field_";
char hash_value_header[] = "godblesspika_hash_value_";

char list_key_header[] = "godblesspika_list_key_";
char list_field_header[] = "godblesspika_list_field_";

char zset_key_header[] = "godblesspika_zset_key_";
char zset_member_header[] = "godblesspika_zset_member_";

char set_key_header[] = "godblesspika_set_key_";
char set_member_header[] = "godblesspika_set_member_";

Nemo *n;
int64_t cnt;

inline int64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

char* gen_str(char *key, char *prefix, int64_t id) {
  //snprintf (key, 256, "%s%010ld", prefix, id);
  snprintf (key, 256, "%s%ld", prefix, id);
  return key;
}
char* gen_str_len(char *key, int len) {
  for (int i = 0; i < len - 1; i++) {
    key[i] = 'a' + rand() % 26;
  }
  key[len - 1] = '\0';
  return key;
}

void* call_Dump(void *arg) {
  int type = *((int *)arg);

  printf ("thread type=%d\n", type);

  Status s;
  std::string sret;
  int64_t iret;

  char key[256];
  char value[256];

  for (int64_t i = 0; i < cnt; i++) {
    if (i % 100000 == 0) printf ("thread type:%d i=%ld\n", type, i);

    switch (type) {
      case 0: {
        string k(gen_str(key, kv_key_header, i));
        string v(gen_str(key, kv_value_header, i));
        n->Set(k, v);
        break;
      }

      case 1: {
        string hash_k(gen_str(key, hash_key_header, i));
        string hash_f(gen_str(key, hash_field_header, i));
        string hash_v(gen_str(key, hash_value_header, i));
        n->HSet(hash_k, hash_f, hash_v);
        break;
      }

      case 2: {
        string list_k(gen_str(key, list_key_header, i));
        string list_f(gen_str(key, list_field_header, i));
        n->LPush(list_k, list_f, &iret);
        break;
      }

      case 3: {
        string zset_k(gen_str(key, zset_key_header, i));
        string zset_m(gen_str(key, zset_member_header, i));
        n->ZAdd(zset_k, i, zset_m, &iret);
        break;
      }

      case 4: {
        string set_k(gen_str(key, set_key_header, i));
        string set_m(gen_str(key, set_member_header, i));
        n->SAdd(set_k, set_m, &iret);
        break;
      }
    }
  }
}

int main(int argc, char *argv[])
{
  nemo::Options options;
  options.target_file_size_base = 20 * 1024 * 1024;
  options.write_buffer_size = 256 * 1024 * 1024;
  

  if (argc < 3) {
    printf ("usage: rush count db_path\n");
    exit (-1);
  }

  char *pEnd;
  cnt = strtoll(argv[1], &pEnd, 10);
  std::string path(argv[2]);

  
  int64_t st = NowMicros();
  printf ("cnt=%ld db_path: %s st_timestamp:%ld\n", cnt, path.c_str(), st);

  n = new Nemo(path.c_str(), options); 

  pthread_t tid[5];

  for (int i = 0; i < 5; i++) {
    int *arg = new int;
    *arg = i;
    if (pthread_create(&tid[i], NULL, &call_Dump, arg) != 0) {
      printf ("pthread create %d failed\n", i);
    }
  }


  void *retval;
  for (int i = 0; i < 5; i++) {
    pthread_join(tid[i], &retval);
  }

  int64_t ed = NowMicros();
  printf ("End ts:%ld\n", ed);

  std::vector<uint64_t> nums;
  n->GetKeyNum(nums);

  for (int i = 0; i < nums.size(); i++) {
    printf (" key_num i=%d %ld\n", i, nums[i]);
  }


  return 0;
}
