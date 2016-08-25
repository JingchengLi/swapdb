#include <iostream>
#include <vector>
#include <string>
#include <ctime>
#include <inttypes.h>
#include <limits>
#include <sys/time.h>

#include "nemo.h"
#include "xdebug.h"

using namespace nemo;
using namespace std;

const int MAXN = 1024;

Nemo *n;
int tn;
int cnt;
int key_num;
int length;
pthread_t tid[MAXN];
int64_t total = 0;

char field[MAXN];
char value[MAXN];

inline int64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

void gen_random(char *s, const int len) {
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  for (int i = 0; i < len; ++i) {
    s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  s[len] = 0;
}

void* ThreadMain1(void *arg) {
  int64_t st, ed, t_sum = 0LL;
  Status s;
  srand(time(NULL));

  for (int i = 0; i < cnt; i++) {
    int key_id = rand() % key_num;

    string key = "hash_key:" + to_string(key_id);
    gen_random(field, length);
    gen_random(value, length);
    st = NowMicros();
    s = n->HSet(key, field, value);
    ed = NowMicros();

    t_sum += ed - st;

    if (i > 0 && i % 10000 == 0) {
      printf ("   tid:%10u run %d request, tot is %10lu\n", pthread_self(), i + 1, t_sum);
    }
  }
  printf ("  tid:%10u end, run %d request, tot is %10lu\n", pthread_self(), cnt, t_sum);
  //total += t_sum;
  return (void *)t_sum;
}

int main(int argc, char* argv[]) {
  if (argc < 5) {
    printf ("Usage: ./bench  thread_num  query_per_thread key_num key_length\n");
    exit(0);
  }

  char *pend;
  tn = strtol(argv[1], &pend, 10);
  cnt = strtol(argv[2], &pend, 10);
  key_num = strtol(argv[3], &pend, 10);
  length = strtol(argv[4], &pend, 10);


  printf ("thread_num %d, query %d per thread, key_num is %d, key(field)_length is %d\n", tn, cnt, key_num, length);

  nemo::Options options;
  options.target_file_size_base = 20 * 1024 * 1024;

  n = new Nemo("./tmp/", options); 

  for (int i = 0; i < tn; i++) {
    pthread_create(&tid[i], NULL, &ThreadMain1, NULL);
  }
    
  void *res;
  for (int i = 0; i < tn; i++) {
      if (pthread_join(tid[i], &(res)) != 0) {
        printf ("pthread_join %d failed\n", tid[i]); 
      } else {
        printf ("thread_join %d return %10u\n", i, (int64_t)res);
      }
      total += (int64_t)res;
  }
  printf ("Total time: %10ld us;\nTotal request: %10ld;\nQPS: %10.3lf\n", total, tn * cnt, (double)1000000.0 * tn * cnt * tn / total);

  delete n;

  return 0;
}
