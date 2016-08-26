#include <iostream>
#include <vector>
#include <string>
#include <ctime>
#include <inttypes.h>
#include <limits>

#include "nemo.h"
#include "xdebug.h"

using namespace nemo;

int main()
{
    nemo::Options options;
    options.target_file_size_base = 20 * 1024 * 1024;

    //options.compression = false; 
    Nemo *n = new Nemo("./tmp/", options); 
    Status s;

    std::string res;

    /*
     *************************************************KV**************************************************
     */

    std::vector<std::string> keys;
    std::vector<KV> kvs;
    std::vector<KVS> kvss;
    std::vector<SM> sms;

    /*
     *  Test Internal Skip
     */
    log_info("======Test Internal Skip======");
    KIterator *kit = n->KScan("", "", -1);
    kit->Skip(2);
    bool skip_ret = kit->Valid();
    log_info ("test Internal Skip(2) when no keys, expect false, return %s", skip_ret ? "true" : "false");

    s = n->Set("tSetKey1", "tSetVal1");
    s = n->Set("tSetKey2", "tSetVal2");
    s = n->Set("tSetKey3", "tSetVal3");

    kit = n->KScan("", "", -1);
    kit->Skip(1);
    skip_ret = kit->Valid();
    log_info ("test Internal Skip(1) when 3 keys, expect true, return %s", skip_ret ? "true" : "false");
    if (skip_ret) {
        log_info ("                 expect[tSetKey2, tSetVal2], key=%s val=%s", kit->key().c_str(), kit->value().c_str());
    }

    kit->Skip(1);
    skip_ret = kit->Valid();
    log_info ("test Internal Skip(1) again when 3 keys, expect true, return %s", skip_ret ? "true" : "false");
    if (skip_ret) {
        log_info ("                 expect[tSetKey3, tSetVal3], key=%s val=%s", kit->key().c_str(), kit->value().c_str());
    }

    kit->Skip(2);
    skip_ret = kit->Valid();
    log_info ("test Internal Skip(2) again when 3 keys, expect false, return %s", skip_ret ? "true" : "false");
    log_info ("                 expect[tSetKey3, tSetVal3], key=%s val=%s", kit->key().c_str(), kit->value().c_str());

    kit = n->KScan("", "", -1);
    kit->Skip(5);
    skip_ret = kit->Valid();
    log_info ("test Internal Skip(5) when 3 keys, expect false, return %s", skip_ret ? "true" : "false");
    log_info ("                 expect[tSetKey3, tSetVal3], key=%s val=%s", kit->key().c_str(), kit->value().c_str());
    log_info("");

    /*
     *  Test Internal Hash Skip
     */
    log_info("======Test Internal Hash Skip======");
    HIterator *hs_it = n->HScan("tHSetKey", "", "", -1);
    hs_it->Skip(2);
    skip_ret = hs_it->Valid();
    log_info ("test Internal Hash Skip(2) when no keys, expect false, return %s", skip_ret ? "true" : "false");

    s = n->HSet("tHSetKey", "tHSetField1", "tSetVal1");
    s = n->HSet("tHSetKey", "tHSetField2", "tSetVal2");
    s = n->HSet("tHSetKey", "tHSetField3", "tSetVal3");

    hs_it = n->HScan("tHSetKey", "", "", -1);
    hs_it->Skip(1);
    skip_ret = hs_it->Valid();
    log_info ("test Internal Hash Skip(1) when 3 keys, expect true, return %s", skip_ret ? "true" : "false");
    if (skip_ret) {
        log_info ("                     expect[tHSetKey, tHSetField2, tSetVal2], key=%s field=%s val=%s", hs_it->key().c_str(), hs_it->field().c_str(), hs_it->value().c_str());
    }

    hs_it->Skip(1);
    skip_ret = hs_it->Valid();
    log_info ("test Internal Hash Skip(1) again when 3 keys, expect true, return %s", skip_ret ? "true" : "false");
    if (skip_ret) {
        log_info ("                     expect[tHSetKey, tHSetField3, tSetVal3], key=%s field=%s val=%s", hs_it->key().c_str(), hs_it->field().c_str(), hs_it->value().c_str());
    }

    hs_it = n->HScan("tHSetKey", "", "", -1);
    hs_it->Skip(5);
    skip_ret = hs_it->Valid();
    log_info ("test Internal Hash Skip(5) when 3 keys, expect false, return %s", skip_ret ? "true" : "false");
    log_info ("                 expect[tHSetKey, tHSetField3, tSetVal3], key=%s field=%s val=%s", hs_it->key().c_str(), hs_it->field().c_str(), hs_it->value().c_str());
    log_info("");

    /*
     *  Test Internal ZSet Skip
     */
    log_info("======Test Internal ZSet Skip======");
    ZIterator *zs_it = n->ZScan("tZSetKey", nemo::ZSET_SCORE_MIN, nemo::ZSET_SCORE_MAX, -1);
    zs_it->Skip(2);
    skip_ret = zs_it->Valid();
    log_info ("test Internal ZSet Skip(2) when no keys, expect false, return %s", skip_ret ? "true" : "false");

    int64_t za_res;
    s = n->ZAdd("tZSetKey", 100.0, "tHMember1", &za_res);
    s = n->ZAdd("tZSetKey", 110.0, "tHMember2", &za_res);
    s = n->ZAdd("tZSetKey", 120.0, "tHMember3", &za_res);

    ZIterator *zit_zset = n->ZScan("tZSetKey", ZSET_SCORE_MIN, ZSET_SCORE_MAX, -1);
    if (zit_zset == NULL) {
        log_info("ZScan error!");
    }
    for (; zit_zset->Valid(); zit_zset->Next()) {
        log_info("Test ZScan key: %s, score: %lf, member: %s", zit_zset->key().c_str(), zit_zset->score(), zit_zset->member().c_str());
    }

    zs_it = n->ZScan("tZSetKey", nemo::ZSET_SCORE_MIN, nemo::ZSET_SCORE_MAX, -1);

    zs_it->Skip(1);
    skip_ret = zs_it->Valid();
    log_info ("test Internal ZSet Skip(1) when 3 keys, expect true, return %s", skip_ret ? "true" : "false");
    if (skip_ret) {
        log_info ("                      expect[tZSetKey, 110, tHMember2], key=%s member=%s score=%lf", zs_it->key().c_str(), zs_it->member().c_str(), zs_it->score());
    }

    zs_it->Skip(1);
    skip_ret = zs_it->Valid();
    log_info ("test Internal ZSet Skip(1) again when 3 keys, expect true, return %s", skip_ret ? "true" : "false");
    if (skip_ret) {
        log_info ("                     expect[tZSetKey, 120, tHMember3], key=%s member=%s score=%lf", zs_it->key().c_str(), zs_it->member().c_str(), zs_it->score());
    }

    zs_it = n->ZScan("tZSetKey", nemo::ZSET_SCORE_MIN, nemo::ZSET_SCORE_MAX, -1);
    zs_it->Skip(5);
    skip_ret = zs_it->Valid();
    log_info ("test Internal ZSet Skip(5) when 3 keys, expect true, return %s", skip_ret ? "true" : "false");
    log_info ("                     expect[tZSetKey, 120, tHMember3], key=%s member=%s score=%lf", zs_it->key().c_str(), zs_it->member().c_str(), zs_it->score());
    log_info("");

    /*
     *  Test Internal Set Skip
     */
    log_info("======Test Internal Set Skip======");
    SIterator *ss_it = n->SScan("tSetKey", -1);
    ss_it->Skip(2);
    skip_ret = ss_it->Valid();
    log_info ("test Internal Set Skip(2) when no keys, expect false, return %s", skip_ret ? "true" : "false");

    s = n->SAdd("tSetKey", "tSetMember1", &za_res);
    s = n->SAdd("tSetKey", "tSetMember2", &za_res);
    s = n->SAdd("tSetKey", "tSetMember3", &za_res);

    ss_it = n->SScan("tSetKey", -1);
    if (ss_it == NULL) {
        log_info("ZScan error!");
    }
    for (; ss_it->Valid(); ss_it->Next()) {
        log_info("Test SScan key: %s, member: %s", ss_it->key().c_str(), ss_it->member().c_str());
    }
    
    ss_it = n->SScan("tSetKey", -1);
    ss_it->Skip(1);
    skip_ret = ss_it->Valid();
    log_info ("test Internal Set Skip(1) when 3 keys, expect true, return %s", skip_ret ? "true" : "false");
    if (skip_ret) {
        log_info ("                      expect[tSetKey,  tSetMember2], key=%s member=%s", ss_it->key().c_str(), ss_it->member().c_str());
    }

    ss_it->Skip(1);
    skip_ret = ss_it->Valid();
    log_info ("test Internal Set Skip(1) again when 3 keys, expect true, return %s", skip_ret ? "true" : "false");
    if (skip_ret) {
        log_info ("                      expect[tSetKey,  tSetMember3], key=%s member=%s", ss_it->key().c_str(), ss_it->member().c_str());
    }

    ss_it = n->SScan("tSetKey", -1);
    ss_it->Skip(5);
    skip_ret = ss_it->Valid();
    log_info ("test Internal Set Skip(5) when 3 keys, expect true, return %s", skip_ret ? "true" : "false");
    log_info ("                      expect[tSetKey,  tSetMember3], key=%s member=%s", ss_it->key().c_str(), ss_it->member().c_str());
    log_info("");

    /*
     *  Test Setrange
     */
    int64_t del_ret;

    log_info("======Test Setrange======");
    s = n->Set("tSetRangeKey", "abcd");
    int64_t sr_len;
    s = n->Setrange("tSetRangeKey", 6, "123", &sr_len);
    std::string sr_val;
    s = n->Get("tSetRangeKey", &sr_val);
    log_info("After Setrange, val = %s, new_len = %ld", sr_val.c_str(), sr_len);
    s = n->Del("tSetRangeKey", &del_ret);
    log_info("");

    /*
     *  Test Set
     */
    log_info("======Test Set======");
    s = n->Set("tSetKey", "tSetVal");
    log_info("Test Set OK return %s", s.ToString().c_str());
    int64_t ttl;
    n->TTL("tSetKey", &ttl);
    log_info("          Defalut set,  TTL is %ld\n", ttl);
    log_info("");

    /*
     *  Test Set with TTL
     */
    log_info("======Test Set with ttl======");
//    s = n->Set("tSetKeyWithTTL", "tSetVal", 7);
//    log_info("Test Set with ttl return %s", s.ToString().c_str());
//
//    //int64_t ttl;
//    for (int i = 0; i < 4; i++) {
//        sleep(3);
//        s = n->Get("tSetKeyWithTTL", &res);
//        log_info("          Set with ttl after %ds, return %s", (i+1)*3, s.ToString().c_str());
//        if (s.ok()) {
//            s = n->TTL("tSetKeyWithTTL", &ttl);
//            log_info("          new TTL is %ld, Get res:%s\n", ttl, res.c_str());
//        }
//        s = n->TTL("tSetKeyWithTTL", &ttl);
//        log_info("          TTL return %s, ttl is %ld\n", s.ToString().c_str(), ttl);
//        log_info("");
//    }
//    log_info("");

    /*
     *  Test Get
     */
    log_info("======Test Get======");
    s = n->Set("tGetKey", "tGetVal");
    res = "";
    s = n->Get("tGetKey", &res);
    log_info("Test Get OK return %s, result tGetVal = %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->Get("tGetNotFoundKey", &res);
    log_info("Test Get NotFound return %s, result NULL = %s", s.ToString().c_str(), res.c_str());
    log_info("");

    /*
     *  Test Setxx
     */
    log_info("======Test Setxx======");
    s = n->Del("a", &del_ret);
    int64_t xx_ret;
    s = n->Setxx("a", "b", &xx_ret);
    log_info("Test Setxx OK return %s, retval: %ld", s.ToString().c_str(), xx_ret);
    s = n->Get("a", &res);
    log_info("After setxx, Get return %s, result res = %s", s.ToString().c_str(), res.c_str());
//    /*
//     *  Test Expire 
//     */
//    int64_t e_ret;
//    log_info("======Test Expire======");
//    s = n->Expire("tSetKey", 7, &e_ret);
//    log_info("Test Expire with key=tSetKey in 7s, return %s", s.ToString().c_str());
//
//    for (int i = 0; i < 3; i++) {
//        sleep(3);
//        s = n->Get("tSetKey", &res);
//        log_info("          after %ds, return %s", (i+1)*3, s.ToString().c_str());
//        if (s.ok()) {
//            n->TTL("tSetKey", &ttl);
//            log_info("          new TTL is %ld, Get res:%s\n", ttl, res.c_str());
//        }
//    }
//    log_info("");
//
//
//    /*
//     *  Test Expireat
//     */
//    log_info("======Test Expireat======");
//    s = n->Set("tSetKey", "tSetVal");
//
//    std::time_t t = std::time(0);
//    s = n->Expireat("tSetKey", t + 8, &e_ret);
//    log_info("Test Expireat with key=tSetKey at timestamp=%ld in 8s, return %s", (t+8), s.ToString().c_str());
//
//    for (int i = 0; i < 3; i++) {
//        sleep(3);
//        s = n->Get("tSetKey", &res);
//        log_info("          after %ds, return %s", (i+1)*3, s.ToString().c_str());
//        if (s.ok()) {
//            n->TTL("tSetKey", &ttl);
//            log_info("          new TTL is %ld, Get res:%s\n", ttl, res.c_str());
//        }
//    }
//    log_info("");
//
//    s = n->Set("tSetKey", "tSetVal");
//    s = n->Expireat("tSetKey", 8, &e_ret);
//    log_info("Test Expireat with key=tSetKey at a passed timestamp=8, return %s", s.ToString().c_str());
//    s = n->Get("tSetKey", &res);
//    log_info("          Get a invalid key return %s, expect ok",  s.ToString().c_str());
//    if (s.IsNotFound()) {
//        n->TTL("tSetKey", &ttl);
//        log_info("          NotFound key's TTL is %ld, Get res:%s\n", ttl, res.c_str());
//    }
//    log_info("");
//
//    /*
//     *  Test Persist 
//     */
//    log_info("======Test Persist======");
//    s = n->Set("tSetKey", "tSetVal");
//    s = n->Expire("tSetKey", 7, &e_ret);
//    log_info("Test Persist with key=tSetKey in 7s, return %s", s.ToString().c_str());
//
//    for (int i = 0; i < 3; i++) {
//        sleep(3);
//        if (i == 1) {
//            s = n->Persist("tSetKey", &e_ret);
//            log_info(" Test Persist return %s", s.ToString().c_str());
//        }
//        s = n->Get("tSetKey", &res);
//        log_info("          after %ds, return %s", (i+1)*3, s.ToString().c_str());
//        if (s.ok()) {
//            n->TTL("tSetKey", &ttl);
//            log_info("          new TTL is %ld, Get res:%s\n", ttl, res.c_str());
//        }
//    }
//    log_info("");
    /*
     *  Test Del
     */
    log_info("======Test Del======");
    s = n->Del("tSetKey", &del_ret);
    log_info("Test Det OK return %s", s.ToString().c_str());
    res = "";
    s = n->Get("tSetKey", &res);
    log_info("After Del, Get NotFound return %s, result tGetVal = %s", s.ToString().c_str(), res.c_str());

    //just delete all key-value set before
    s = n->Del("tGetKey", &del_ret);
    log_info("");

    /*
     *  Test MGet
     */
    log_info("======Test MGet======");
    keys.clear();
    kvss.clear();
    keys.push_back("tMGetKey1");
    keys.push_back("tMGetKey2");
    keys.push_back("tMGetNotFoundKey");
    s = n->Set("tMGetKey1", "tMGetVal1");
    s = n->Set("tMGetKey2", "tMGetVal1");

    s = n->MGet(keys, kvss);
    std::vector<KVS>::iterator kvs_iter;
    for (kvs_iter = kvss.begin(); kvs_iter != kvss.end(); kvs_iter++) {
        log_info("Test MGet OK return %s, key: %s, val: %s status: %s", s.ToString().c_str(), kvs_iter->key.c_str(), kvs_iter->val.c_str(), kvs_iter->status.ToString().c_str());
    }
    log_info("");

    /*
     *  Test MDel
     */
    log_info("======Test MDel======");
    int64_t mcount;
    s = n->MDel(keys, &mcount);
    log_info("Test MDel OK return %s, mcount is %ld", s.ToString().c_str(), mcount);
    //After MDel, all should return NotFound
    kvss.clear();
    s = n->MGet(keys, kvss);
    for (kvs_iter = kvss.begin(); kvs_iter != kvss.end(); kvs_iter++) {
        log_info("After MDel, Test MGet OK return %s, key: %s, val: %s status: %s", s.ToString().c_str(), kvs_iter->key.c_str(), kvs_iter->val.c_str(), kvs_iter->status.ToString().c_str());
    }
    log_info("");

    /*
     *  Test MSet
     */
    log_info("======Test MSet======");
    kvs.clear();
    kvs.push_back({"tMSetKey1", "tMSetVal1mm"});
    kvs.push_back({"tMSetKey2", "tMSetVal2mm"});
    kvs.push_back({"tMSetKey3", "tMSetVal3mm"});
    s = n->MSet(kvs);
    log_info("Test MSet OK return %s", s.ToString().c_str());

    keys.clear();
    keys.push_back("tMSetKey1");
    keys.push_back("tMSetKey2");
    keys.push_back("tMSetKey3");
    kvss.clear();
    s = n->MGet(keys, kvss);
    for (kvs_iter = kvss.begin(); kvs_iter != kvss.end(); kvs_iter++) {
        log_info("After MSet, Test MGet OK return %s, key: %s, val: %s status: %s", s.ToString().c_str(), kvs_iter->key.c_str(), kvs_iter->val.c_str(), kvs_iter->status.ToString().c_str());
    }

    //just delete all key-value set before
    s = n->MDel(keys, &mcount);
    log_info("");

    /*
     *  Test Incrby
     */
    log_info("======Test Incrby======");
    s = n->Set("tIncrByKey", "012");
    res = "";
    s = n->Incrby("tIncrByKey", 6, res);
    log_info("Test Incrby OK return %s, 12 Incrby 6 val: %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->Incrby("tIncrByKey", -2, res);
    log_info("Test Incrby OK return %s, 18 Incrby -2 val: %s", s.ToString().c_str(), res.c_str());

    s = n->Incrby("tIncrByKey", LLONG_MAX, res);
    log_info("Test Incrby OK return %s, Incrby LLONG_MAX, expect overflow", s.ToString().c_str());

    //Test NonNum key IncrBy
    s = n->Set("tIncrByKey", "NonNum");
    res = "";
    s = n->Incrby("tIncrByKey", 6, res);
    log_info("Test Incrby OK return %s, NonNum Incrby 6 val: %s", s.ToString().c_str(), res.c_str());

    /*
     *  Test Decrby
     */
    log_info("======Test Decrby======");
    s = n->Set("tIncrByKey", "012");
    res = "";
    s = n->Decrby("tIncrByKey", 6, res);
    log_info("Test Decrby OK return %s, 12 Decrby 6 val: %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->Decrby("tIncrByKey", -2, res);
    log_info("Test Decrby OK return %s, 6 Decrby -2 val: %s", s.ToString().c_str(), res.c_str());

    s = n->Decrby("tIncrByKey", LLONG_MIN, res);
    log_info("Test Decrby OK return %s, Decrby LLONG_MIN, expect overflow", s.ToString().c_str());

    //Test NonNum key IncrBy
    s = n->Set("tIncrByKey", "NonNum");
    res = "";
    s = n->Decrby("tIncrByKey", 6, res);
    log_info("Test Decrby OK return %s, NonNum Decrby 6 val: %s", s.ToString().c_str(), res.c_str());

    //just delete all key-value set before
    s = n->Del("tIncrByKey", &del_ret);
    log_info("");

    /*
     *  Test Incrbyfloat
     */
    log_info("======Test Incrbyfloat======");
    s = n->Set("tIncrByKey", "012");
    res = "";
    s = n->Incrbyfloat("tIncrByKey", 6.0, res);
    log_info("Test Incrbyfloat OK return %s, 12 Incrbyfloat 6.0 val: %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->Incrbyfloat("tIncrByKey", -2.3, res);
    log_info("Test Incrbyfloat OK return %s, 18 Incrbyfloat -2.3 val: %s", s.ToString().c_str(), res.c_str());

    s = n->Incrbyfloat("tIncrByKey", std::numeric_limits<double>::max(), res);
    s = n->Incrbyfloat("tIncrByKey", std::numeric_limits<double>::max(), res);
    log_info("Test Incrbyfloat OK return %s, Incrbyfloat numric_limits max , expect overflow", s.ToString().c_str());

    s = n->Set("tIncrByKey", "+12");
    res = "";
    s = n->Incrbyfloat("tIncrByKey", 6.0, res);
    log_info("Test Incrbyfloat with +12 OK return %s, +12 Incrbyfloat 6.0 val: %s", s.ToString().c_str(), res.c_str());

    res = "";
    s = n->Incrbyfloat("NotExistKey", 6.0, res);
    log_info("Test Incrbyfloat with NotExistKey OK return %s, Incrbyfloat 6.0 val: %s", s.ToString().c_str(), res.c_str());

    sr_val = "";
    s = n->Get("NotExistKey", &sr_val);
    log_info("     After Incrbyflost NotExistKey, return %s, val = %s, new_len = %ld", s.ToString().c_str(), sr_val.c_str(), sr_len);

    //Test NonNum key IncrBy
    s = n->Set("tIncrByKey", "NonNum");
    res = "";
    s = n->Incrbyfloat("tIncrByKey", 6, res);
    log_info("Test Incrbyfloat OK return %s, NonNum Incrbyfloat 6 expect value not float", s.ToString().c_str());

    //just delete all key-value set before
    s = n->Del("tIncrByKey", &del_ret);
    log_info("");
    //char ch;
    //scanf ("%c", &ch);
    
    /*
     *  Test GetSet
     */
    log_info("======Test GetSet======");
    s = n->Set("tGetSetKey", "tGetSetVal");
    res = "";
    s = n->GetSet("tGetSetKey", "tGetSetVal_new", &res);
    log_info("Test GetSet OK return %s, old_val: %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->Get("tGetSetKey", &res);
    log_info("After GetSet, Test Get OK return %s, val: %s", s.ToString().c_str(), res.c_str());

    // test NonExist key GetSet;
    res = "";
    s = n->GetSet("tGetSetNotFoundKey", "tGetSetNotFoundVal_new", &res);
    log_info("Test GetSet OK return %s, old_val: %s", s.ToString().c_str(), res.c_str());
    s = n->Get("tGetSetNotFoundKey", &res);
    log_info("After GetSet, Test Get OK return %s, val: %s", s.ToString().c_str(), res.c_str());

    //just delete all key-value set before
    s = n->Del("tGetSetKey", &del_ret);
    s = n->Del("tGetSetNotFoundKey", &del_ret);
    log_info("");

    /*
     *  Test Getrange
     */
    log_info("======Test Getrange======");
    s = n->Set("tGetrangekey", "abcd");
    std::string substr;
    s = n->Getrange("tGetrangekey", 0, -1, substr);
    log_info("substr: %s", substr.c_str());

    
    /*
     *  Test Scan
     */
    log_info("======Test Scan======");
    kvs.clear();
    kvs.push_back({"tScanKey1", "tScanVal1"});
    kvs.push_back({"tScanKey2", "tScanVal2"});
    kvs.push_back({"tScanKey21", "tScanVal21"});
    kvs.push_back({"tScanKey3", "tScanVal3"});
    
    keys.clear();
    keys.push_back("tScanKey1");
    keys.push_back("tScanKey2");
    keys.push_back("tScanKey2");

    s = n->MSet(kvs);
    KIterator *scan_iter = n->KScan("tScanKey1", "tScanKey2", -1);
    if (scan_iter == NULL) {
        log_info("Scan error!");
    }
    for (; scan_iter->Valid(); scan_iter->Next()) {
        log_info("Test Scan key: %s, value: %s", scan_iter->key().c_str(), scan_iter->value().c_str());
    }

    //just delete all key-value set before
    s = n->MDel(keys, &mcount);
    log_info("");

    res = "";
    keys.clear();
    kvs.clear();
    kvss.clear();



    /*
     *************************************************HASH**************************************************
     */
    std::vector<std::string> fields;
    std::vector<std::string> values;
    std::vector<FV> fvs;
    std::vector<FVS> fvss;
    
    /*
     *  Test HSet
     */
    log_info("======Test HSet======");
    s = n->HSet("tHSetKey", "song", "tHSetVal");
    log_info("Test HSet OK return %s", s.ToString().c_str());
    log_info("");

    /*
     *  Test HGet
     */
    log_info("======Test HGet======");
    s = n->HSet("tHGetKey", "song", "tGetVal");
    res = "";
    s = n->HGet("tHGetKey", "song", &res);
    log_info("Test HGet OK return %s, result tHGetVal = %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->HGet("tHGetNotFoundKey", "song", &res);
    log_info("Test Get NotFound return %s, result NULL = %s", s.ToString().c_str(), res.c_str());
    s = n->HGet("tHGetKey", "non-field", &res);
    log_info("Test Get NotFound return %s, result NULL = %s", s.ToString().c_str(), res.c_str());
    log_info("");

    /*
     *  Test HDel
     */
    log_info("======Test HDel======");
    s = n->HDel("tHSetKey", "song");
    log_info("Test HDet OK return %s", s.ToString().c_str());
    res = "";
    s = n->HGet("tHSetKey", "song", &res);
    log_info("After HDel, HGet NotFound return %s, result tGetVal = %s", s.ToString().c_str(), res.c_str());

    //just delete all key-value set before
    s = n->HDel("tHGetKey", "song");
    log_info("");

    /*
     *  Test HExists
     */
    log_info("======Test HExists======");
    s = n->HSet("tHExistsKey", "song", "tHExistsVal");
    log_info("test HExists with existed key & field return: %d", n->HExists("tHExistsKey", "song"));
    log_info("test HExists with non-existed key return: %d", n->HExists("tHExistsNonKey", "song"));
    log_info("test HExists with non-existed fields return: %d", n->HExists("tHExistsKey", "non-field"));
    
    //just delete all key-value set before
    s = n->HDel("tHExistsKey", "song");
    log_info("");

    /*
     *  Test HStrlen
     */
    log_info("======Test HStrlen======");
    s = n->HSet("tHStrlenKey", "song", "tHStrlenVal");
    log_info("test HStrlen return 11 = %ld", n->HStrlen("tHStrlenKey", "song"));
    
    //just delete all key-value set before
    s = n->HDel("tHStrlenKey", "song");
    log_info("");

    /*
     *  Test HSetnx
     */
    log_info("======Test HSetnx======");
    s = n->HSetnx("tHSetnxKey", "song", "tHSetnxVal");
    log_info("test HSetnx with non-existed key | fields return %s", s.ToString().c_str());
    s = n->HSetnx("tHSetnxKey", "song", "tHSetnxVal");
    log_info("test HSetnx with existed key | fields return %s", s.ToString().c_str());

    //just delete all key-value set before
    s = n->HDel("tHSetnxKey", "song");
    log_info("");

    /*
     *  Test HIncrby
     */
    log_info("======Test HIncrby======");
    s = n->HSet("tHIncrByKey", "song", "12");
    res = "";
    s = n->HIncrby("tHIncrByKey", "song", 6, res);
    log_info("Test HIncrby OK return %s, 12 HIncrby 6 val: %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->HIncrby("tHIncrByKey", "song", -2, res);
    log_info("Test HIncrby OK return %s, 18 HIncrby -2 val: %s", s.ToString().c_str(), res.c_str());
    s = n->HIncrby("tHIncrByKey", "song", LLONG_MAX, res);
    log_info("Test HIncrby OK return %s, HIncrby LLONG_MAX", s.ToString().c_str());

    //Test NonNum key HIncrBy
    s = n->HSet("tHIncrByKey", "song", "NonNum");
    res = "";
    s = n->HIncrby("tHIncrByKey", "song", 6, res);
    log_info("Test HIncrby OK return %s, NonNum HIncrby 6 val: %s", s.ToString().c_str(), res.c_str());

    //just delete all key-value set before
    s = n->HDel("tHIncrByKey", "song");
    log_info("");

    /*
     *  Test HIncrbyfloat
     */
    log_info("======Test HIncrbyfloat======");
    s = n->HSet("tHIncrByfloatKey", "song", "12.1");
    res = "";
    s = n->HIncrbyfloat("tHIncrByfloatKey", "song", 6.2, res);
    log_info("Test HIncrbyfloat OK return %s, 12.1 HIncrbyfloat 6.2 val: %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->HIncrbyfloat("tHIncrByfloatKey", "song", -2.1, res);
    log_info("Test HIncrbyfloat OK return %s, 18.3 HIncrbyfloat -2.1 val: %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->HIncrbyfloat("tHIncrByfloatKey", "nonExist", 5.0, res);
    log_info("Test HIncrbyfloat OK return %s, nonExist HIncrbyfloat 5.0 val: %s", s.ToString().c_str(), res.c_str());

    s = n->HIncrbyfloat("tHIncrByfloatKey", "song", std::numeric_limits<double>::max(), res);
    s = n->HIncrbyfloat("tHIncrByfloatKey", "song", std::numeric_limits<double>::max(), res);
    log_info("Test HIncrbyfloat OK return %s, HIncrbyfloat max float, expect overflow", s.ToString().c_str());

    //Test NonNum key HIncrBy
    s = n->HSet("tHIncrByfloatKey", "song", "NonNum");
    res = "";
    s = n->HIncrby("tHIncrByfloatKey", "song", 6, res);
    log_info("Test HIncrbyfloat OK return %s, NonNum HIncrbyfloat 6 val: %s", s.ToString().c_str(), res.c_str());
    log_info("");
    //scanf("%c", &ch);

    /*
     *  Test HMGet
     */
    log_info("======Test MGet======");
    fields.clear();
    fvss.clear();
    fields.push_back("tHMGetField1");
    fields.push_back("tHMGetField2");
    fields.push_back("tHMGetField3");
    fields.push_back("tHMGetNotFoundField");
    s = n->HSet("tHMGetKey", "tHMGetField1", "tHMGetVal1");
    s = n->HSet("tHMGetKey", "tHMGetField2", "tHMGetVal2");
    s = n->HSet("tHMGetKey", "tHMGetField3", "tHMGetVal3");

    s = n->HMGet("tHMGetKey", fields, fvss);
    std::vector<FVS>::iterator fvs_iter;
    for (fvs_iter = fvss.begin(); fvs_iter != fvss.end(); fvs_iter++) {
        log_info("Test HMGet OK return %s, field: %s, val: %s status: %s", s.ToString().c_str(), fvs_iter->field.c_str(), fvs_iter->val.c_str(), fvs_iter->status.ToString().c_str());
    }
    log_info("");

    /*
     *  Test HKeys
     */
    log_info("======Test HKeys======");
    fields.clear();
    s = n->HKeys("tHMGetKey", fields);
    log_info("Test Hkey OK return %s", s.ToString().c_str());
    std::vector<std::string>::iterator field_iter;
    for (field_iter = fields.begin(); field_iter != fields.end(); field_iter++) {
        log_info("Test HKeys field: %s", (*field_iter).c_str());
    }
    log_info("");

    /*
     *  Test HGetall
     */
    log_info("======Test HGetall======");
    fvs.clear();
    s = n->HGetall("tHMGetKey", fvs);
    log_info("Test HGetall OK return %s", s.ToString().c_str());
    std::vector<FV>::iterator fv_iter;
    for (fv_iter = fvs.begin(); fv_iter != fvs.end(); fv_iter++) {
        log_info("Test HGetall, field: %s, val: %s", fv_iter->field.c_str(), fv_iter->val.c_str());
    }
    log_info("");
    
    /*
     *  Test HVals
     */
    log_info("======Test HVals======");
    values.clear();
    s = n->HVals("tHMGetKey", values);
    log_info("Test HVals OK return %s", s.ToString().c_str());
    std::vector<std::string>::iterator value_iter;
    for (value_iter = values.begin(); value_iter != values.end(); value_iter++) {
        log_info("Test HVals field: %s", (*value_iter).c_str());
    }
    log_info("");
    
    /*
     *  Test HLen
     */
    log_info("======Test HLen======");
    log_info("HLen with existed key return: %ld", n->HLen("tHMGetKey"));
    log_info("HLen with non-existe key return: %ld", n->HLen("non-exist"));
    log_info("");

    /*
     *  Test HScan
     */
    log_info("======Test HScan======");

    HIterator *hit = n->HScan("tHMGetKey", "", "", -1);
    if (hit == NULL) {
        log_info("HScan error!");
    }
    for (; hit->Valid(); hit->Next()) {
        log_info("HScan key: %s, field: %s, value: %s", hit->key().c_str(), hit->field().c_str(), hit->value().c_str());
    }
    log_info("");
    //just delete all key-value set before
    s = n->HDel("tHMGetKey1", "tHMGetVal1");
    s = n->HDel("tHMGetKey2", "tHMGetVal2");
    s = n->HDel("tHMGetKey3", "tHMGetVal3");

    /*
     *************************************************List**************************************************
     */
    std::vector<IV> ivs;

    /*
     *  Test LPush
     */
    log_info("======Test LPush======");
    int64_t llen = 0;
    s = n->LPush("tLPushKey", "tLPushVal1", &llen);
    log_info("Test LPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->LPush("tLPushKey", "tLPushVal2", &llen);
    log_info("Test LPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->LPush("tLPushKey", "tLPushVal3", &llen);
    log_info("Test LPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->LPush("tLPushKey", "tLPushVal4", &llen);
    log_info("Test LPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->LPush("tLPushKey", "tLPushVal5", &llen);
    log_info("Test LPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->LPush("tLPushKey", "tLPushVal6", &llen);
    log_info("Test LPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    log_info("");

    /*
     *  Test LLen
     */
    log_info("======Test LLen======");
    n->LLen("tLPushKey", &llen);
    log_info("Test LLen return %ld", llen);
    log_info("");

    /*
     *  Test LPop
     */
    log_info("======Test LPop======");
    res = "";
    s = n->LPop("tLPushKey", &res);
    log_info("Test LPop OK return %s, res = %s", s.ToString().c_str(), res.c_str());
    n->LLen("tLPushKey", &llen);
    log_info("After LPop, Test LLen return %ld", llen);
    log_info("");

  /*
   *  Test LPushx
   */
    log_info("======Test LPushx======");
    s = n->LPushx("tLPushKey", "tLpushval4", &llen);
    log_info("Test LPushx OK return %s", s.ToString().c_str());
    n->LLen("tLPushKey", &llen);
    log_info("After LPushx push an existed key, LLen return %ld", llen);

    s = n->LPushx("not-exist-key", "tLpushval4", &llen);
    log_info("Test LPushx push an non-exist key , LPushx OK return %s", s.ToString().c_str());
    n->LLen("tLPushKey", &llen);
    log_info("After LPushx push an non-exist key, LLen return %ld", llen);

    log_info("");

    /*
     *  Test LRange
     */
    log_info("======Test LRange======");
    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    log_info("Test LRange OK return %s", s.ToString().c_str());
    std::vector<IV>::iterator iter_iv;
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("Test LRange index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    /*
     *  Test LSet
     */
    log_info("======Test LSet======");
    s = n->LSet("tLPushKey", -2, "tLSetVal1");
    log_info("Test LSet OK return %s", s.ToString().c_str());
    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("After LSet, LRange index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    /*
     *  Test LTrim
     */
    log_info("======Test LTrim======");
    s = n->LTrim("tLPushKey", -5, -5);
    log_info("Test LTrim OK return %s", s.ToString().c_str());
    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("After LTrim, LRange index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");
    //just clear the list
    s = n->LTrim("tLPushKey", 1, 0);

    /*
     *  Test RPush
     */
    log_info("======Test RPush======");
    s = n->RPush("tLPushKey", "tLPushVal1", &llen);
    log_info("Test RPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->RPush("tLPushKey", "tLPushVal2", &llen);
    log_info("Test RPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->RPush("tLPushKey", "tLPushVal3", &llen);
    log_info("Test RPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->RPush("tLPushKey", "tLPushVal4", &llen);
    log_info("Test RPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->RPush("tLPushKey", "tLPushVal5", &llen);
    log_info("Test RPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->RPush("tLPushKey", "tLPushVal6", &llen);
    log_info("Test RPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    n->LLen("tLPushKey", &llen);
    log_info("After RPush LLen return %ld", llen);
    log_info("");
    
    /*
     *  Test RPop
     */
    log_info("======Test RPop======");
    res = "";
    s = n->RPop("tLPushKey", &res);
    log_info("Test RPop OK return %s, res = %s", s.ToString().c_str(), res.c_str());
    n->LLen("tLPushKey", &llen);
    log_info("After RPop, Test LLen return %ld", llen);
    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("After RPop, LRange index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");


    /*
     *  Test RPushx
     */
    log_info("======Test RPushx======");
    s = n->RPushx("tLPushKey", "tLpushval4", &llen);
    log_info("Test RPushx OK return %s", s.ToString().c_str());
    n->LLen("tLPushKey", &llen);
    log_info("After RPushx push an existed key, LLen return %ld", llen);
    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("After RPushx push an existed key, LRange index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }

    s = n->LPushx("not-exist-key", "tLpushval4", &llen);
    log_info("Test RPushx push an non-exist key , LPushx OK return %s", s.ToString().c_str());
    n->LLen("tLPushKey", &llen);
    log_info("After RPushx push an non-exist key, LLen return %ld", llen);

    /*
     *  Test RPopLPush
     */
    log_info("======Test RPopLPush======");
    s = n->RPopLPush("tLPushKey", "tLPushKey", res);
    log_info("Test RPopLPush OK return %s", s.ToString().c_str());
    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("After RPushx push an existed key, LRange index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }

    //just clear the list
    s = n->LTrim("tLPushKey", 1, 0);
    log_info("");

    log_info("======Test LInsert======");
    s = n->LTrim("tLPushKey", 1, 0);
    s = n->LTrim("tLPushKey2", 1, 0);
    s = n->LPush("tLPushKey", "tLPushVal1", &llen);
    s = n->LPush("tLPushKey", "tLPushVal2", &llen);
    s = n->LInsert("tLPushKey", AFTER, "tLPushVal2", "newkey", &llen);

    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    log_info("Test LInsert OK return %s", s.ToString().c_str());
    log_info("  insert between two keys, expect: [tLPushVal2 newkey tLPushVal1]");
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("Test LInsert index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    s = n->LPush("tLPushKey2", "tLPushVal1", &llen);
    s = n->LInsert("tLPushKey2", BEFORE, "tLPushVal1", "newkey", &llen);

    log_info("  insert before left, expect: [newkey tLPushVal1]");
    ivs.clear();
    s = n->LRange("tLPushKey2", 0, -1, ivs);
    log_info("Test LInsert OK return %s", s.ToString().c_str());
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("Test LInsert index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    s = n->LInsert("tLPushKey2", AFTER, "tLPushVal1", "newkeyright", &llen);

    log_info("  insert after right, expect: [newkey tLPushVal1 newkeyright]");
    ivs.clear();
    s = n->LRange("tLPushKey2", 0, -1, ivs);
    log_info("Test LInsert OK return %s", s.ToString().c_str());
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("Test LInsert index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    s = n->LInsert("tLPushKey2", AFTER, "newkeyright", "newkeyright2", &llen);

    log_info("  insert after right again, expect: [newkey tLPushVal1 newkeyright newkeyright2]");
    ivs.clear();
    s = n->LRange("tLPushKey2", 0, -1, ivs);
    log_info("Test LInsert OK return %s", s.ToString().c_str());
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("Test LInsert index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    log_info("======Test LRem==========================================================");
    s = n->LTrim("LRemKey", 1, 0);
  //s = n->LTrim("LRemKey2", 1, 0);
    s = n->LPush("LRemKey", "LRemVal1", &llen);
    s = n->LPush("LRemKey", "LRemVal2", &llen);
    s = n->LPush("LRemKey", "LRemVal1", &llen);
    s = n->LPush("LRemKey", "LRemVal2", &llen);
    s = n->LRem("LRemKey", 0, "LRemVal2", &llen);
    log_info("Test LRem OK return %s", s.ToString().c_str());

    ivs.clear();
    s = n->LRange("LRemKey", 0, -1, ivs);
    if (!s.ok()) {
      log_info("Test LRem LRange (0, -1) return %s", s.ToString().c_str());
    }
    log_info("  rem all LRemVal2, expect: [LRemVal1 LRemVal1]");
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
      log_info("Test LRem index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    s = n->LRem("LRemKey", -1, "LRemVal1", &llen);
    log_info("Test LRem OK return %s", s.ToString().c_str());
    ivs.clear();
    s = n->LRange("LRemKey", 0, -1, ivs);
    if (!s.ok()) {
      log_info("Test LRem LRange (0, -1) return %s", s.ToString().c_str());
    }
    log_info("  rem -1 LRemVal1, expect: [LRemVal1]");
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
      log_info("Test LRem index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    s = n->LRem("LRemKey", -1, "LRemVal1", &llen);
    log_info("Test LRem OK return %s", s.ToString().c_str());
    ivs.clear();
    s = n->LRange("LRemKey", 0, -1, ivs);
    if (!s.ok()) {
      log_info("Test LRem LRange (0, -1) return %s", s.ToString().c_str());
    }
    log_info("  rem -1 LRemVal1, expect: []");
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
      log_info("Test LRem index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");
    /*
     *************************************************ZSet**************************************************
     */

    /*
     *  Test ZAdd
     */
    log_info("======Test ZAdd======");
    int64_t zadd_res;
    s = n->ZAdd("tZAddKey", 0, "tZAddMem0", &zadd_res);
    s = n->ZAdd("tZAddKey", 1.1, "tZAddMem1", &zadd_res);
    s = n->ZAdd("tZAddKey", 1.2, "tZAddMem1_2", &zadd_res);
    s = n->ZAdd("tZAddKey", 2.1, "tZAddMem2", &zadd_res);
    s = n->ZAdd("tZAddKey", 2.2, "tZAddMem2_2", &zadd_res);
    s = n->ZAdd("tZAddKey", 3.1, "tZAddMem3", &zadd_res);
    s = n->ZAdd("tZAddKey", 7.1, "tZAddMem7", &zadd_res);
    log_info("Test ZAdd OK return %s", s.ToString().c_str());
    log_info("");

    /*
     *  Test ZCard
     */
    log_info("======Test ZCard======");
    log_info("Test ZCard, return %ld", n->ZCard("tZAddKey"));
    log_info("");
    
    /*
     *  Test ZScan
     */
    log_info("======Test Zscan======");
//    ZIterator *it_zset = n->ZScan("tZAddKey", 0, 10, -1);
    ZIterator *it_zset = n->ZScan("tZAddKey", ZSET_SCORE_MIN, ZSET_SCORE_MAX, -1);
    if (it_zset == NULL) {
        log_info("ZScan error!");
    }
    for (; it_zset->Valid(); it_zset->Next()) {
        log_info("Test ZScan key: %s, score: %lf, member: %s", it_zset->key().c_str(), it_zset->score(), it_zset->member().c_str());
    }

    /*
     *  Test ZCount
     */
    log_info("======Test ZCount======");
    log_info("Test ZCount, return %ld", n->ZCount("tZAddKey", -1, 3)); 
    log_info("");

    /*
     *  Test ZIncrby
     */
    log_info("======Test ZIncrby======");
    std::string new_s;
    s = n->ZIncrby("tZAddKey", "tZAddMem1", 5, new_s);
    log_info("Test ZIncrby with exist key OK return %s, new score is %s", s.ToString().c_str(), new_s.c_str());
    s = n->ZIncrby("tZAddKey", "tZAddMem_ne", 7, new_s);
    log_info("Test ZIncrby with non-exist key OK return %s, new score is %s", s.ToString().c_str(), new_s.c_str());
    it_zset = n->ZScan("tZAddKey", 0, 10, -1);
    if (it_zset == NULL) {
        log_info("ZScan error!");
    }
    for (; it_zset->Valid(); it_zset->Next()) {
        log_info("After ZIncrby, Scan key: %s, score: %lf, value: %s", it_zset->key().c_str(), it_zset->score(), it_zset->member().c_str());
    }
    log_info("After ZIncrby, ZCard return %ld", n->ZCard("tZAddKey")); 
    log_info("");

    /*
     *  Test ZRangebyscore
     */
    log_info("======Test ZRangebyscore======");
    s = n->ZAdd("zk1", 1.1, "m1", &zadd_res);
    s = n->ZAdd("zk1", 1.2, "m2", &zadd_res);
    s = n->ZAdd("zk1", 1.3, "m3", &zadd_res);
    s = n->ZAdd("zk1", 1.4, "m4", &zadd_res);
    s = n->ZAdd("zk1", 1.5, "m5", &zadd_res);
    s = n->ZAdd("zk1", 1.6, "m6", &zadd_res);
    s = n->ZAdd("zk1", 1.7, "m7", &zadd_res);
    s = n->ZAdd("zk1", 1.8, "m8", &zadd_res);
    s = n->ZAdd("zk1", 1.9, "m9", &zadd_res);
    sms.clear();
    s = n->ZRangebyscore("zk1", 1, 1.5, sms, true, true);
//    s = n->ZRangebyscore("tZAddKey", ZSET_SCORE_MIN, ZSET_SCORE_MAX, sms);
    std::vector<SM>::iterator it_sm;
    for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
        log_info("Test ZRangebyscore score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
    }

    /*
     *  Test ZScore
     */
    double score;
    s = n->ZScore("zk1", "m6", &score);
    log_info("Test ZScore return %s, score: %lf", s.ToString().c_str(), score);
    
//    ZLexIterator* zlex = n->ZScanbylex("zk1", "m2", "", -1);
//    for (; zlex->Valid(); zlex->Next()) {
//        log_info("ZScanbylex, Scan key: %s, value: %s", zlex->key().c_str(), zlex->member().c_str());
//    }
    
    /*
     *  Test ZRange
     */
    log_info("======Test ZRange======");
    sms.clear();
    s = n->ZRange("tZAddKey", 0, -1, sms);
    log_info("ZRange return %s", s.ToString().c_str());
    for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
        log_info("Test ZRange score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
    }
    log_info("");

    /*
     *  Test ZRem
     */
    log_info("======Test ZRem======");
    s = n->ZAdd("tZRemKey", 0, "member1", &zadd_res);
    s = n->ZAdd("tZRemKey", 10.0, "member2", &zadd_res);
    s = n->ZAdd("tZRemKey", 10.0, "member3", &zadd_res);

    int64_t zrem_res;
    s = n->ZRem("tZRemKey", "member2", &zrem_res);
    log_info("Test ZRem with exist member return %s, expect [member1=0, member3=10.0]", s.ToString().c_str());
    sms.clear();
    s = n->ZRange("tZRemKey", 0, -1, sms);
    for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
        log_info("          score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
    }
    log_info("");

    s = n->ZRem("tZRemKey", "member2", &zrem_res);
    log_info("Test ZRem with nonexist member return %s, expect [member1=0, member3=10.0]", s.ToString().c_str());
    sms.clear();
    s = n->ZRange("tZRemKey", 0, -1, sms);
    for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
        log_info("          score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
    }
    log_info("");

    /*
     *  Test ZUnionStore
     */
    log_info("======Test ZUnionStore======");
    s = n->ZAdd("tZUnionKey1", 0, "member1", &zadd_res);
    s = n->ZAdd("tZUnionKey1", 1.0, "member2", &zadd_res);
    s = n->ZAdd("tZUnionKey1", 2.0, "member3", &zadd_res);
    s = n->ZAdd("tZUnionKey1", 3.0, "member4", &zadd_res);


    s = n->ZAdd("tZUnionKey2", 1.0, "member1", &zadd_res);
    s = n->ZAdd("tZUnionKey2", 2.5, "member2", &zadd_res);
    s = n->ZAdd("tZUnionKey2", 2.9, "member3", &zadd_res);

    s = n->ZAdd("tZUnionKey3", 2.5, "member1", &zadd_res);

    keys.clear();
    keys.push_back("tZUnionKey1");
    keys.push_back("tZUnionKey2");

    std::vector<double> weights;
    weights.push_back(10);
    weights.push_back(1);

    int64_t union_ret;
    s = n->ZUnionStore("tZUnionNewKey1", 2, keys, weights, Aggregate::SUM, &union_ret);
    if (!s.ok()) {
        log_info("Test ZUnionStore [newkey1, key1, key2] return %s", s.ToString().c_str());
    } else {
        sms.clear();
        s = n->ZRange("tZUnionNewKey1", -10, -1, sms);
        log_info(" ZUnionStore OK, union (10 * key1, 1 * key2) should be [member1=1, member2=12.5, member3=22.9,member4=30]");
        for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
          log_info("            score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
        }
    }

    weights.clear();
    s = n->ZUnionStore("tZUnionNewKey2", 2, keys, weights, Aggregate::MAX, &union_ret);
    if (!s.ok()) {
        log_info("Test ZUnionStore [newkey2, key1, key2] return %s", s.ToString().c_str());
    } else {
        sms.clear();
        s = n->ZRange("tZUnionNewKey2", -10, -1, sms);
        log_info(" ZUnionStore OK, union MAX(1 * key1, 1 * key2) should be [member1=1, member2=2.5, member3=2.9,member4=3]");
        for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
          log_info("            score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
        }
    }
    log_info("");

    /*
     *  Test ZInterStore
     */

    log_info("======Test ZInterStore======");
    
//    log_info("  ZCard, tZUnionKey1 return %ld", n->ZCard("tZUnionKey1"));
//    sms.clear();
//    s = n->ZRange("tZUnionKey1", -9, -1, sms);
//    log_info("ZRange return %s", s.ToString().c_str());
//    for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
//        log_info("Test ZRange score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
//    }
//    log_info("");
//    log_info("  ZCard, tZUnionKey2 return %ld", n->ZCard("tZUnionKey2"));
//    sms.clear();
//    s = n->ZRange("tZUnionKey2", -9, -1, sms);
//    log_info("ZRange return %s", s.ToString().c_str());
//    for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
//        log_info("Test ZRange score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
//    }
//    log_info("");
//    log_info("  ZCard, tZUnionKey3 return %ld", n->ZCard("tZUnionKey3"));
//    sms.clear();
//    s = n->ZRange("tZUnionKey3", -9, -1, sms);
//    log_info("ZRange return %s", s.ToString().c_str());
//    for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
//        log_info("Test ZRange score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
//    }
//    log_info("");

    keys.clear();
    keys.push_back("tZUnionKey1");
    keys.push_back("tZUnionKey2");

    weights.push_back(10);
    weights.push_back(1);

    s = n->ZInterStore("tZInterNewKey1", 2, keys, weights, Aggregate::SUM, &union_ret);
    if (!s.ok()) {
        log_info("Test ZInterStore [newkey1, key1, key2] return %s", s.ToString().c_str());
    } else {
        sms.clear();
        s = n->ZRange("tZInterNewKey1", -10, -1, sms);
        log_info(" ZInterStore OK, inter (10 * key1, 1 * key2) should be [member1=1, member2=12.5, member3=22.9]");
        for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
          log_info("            score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
        }
    }

    keys.push_back("tZUnionKey3");
    s = n->ZInterStore("tZInterNewKey2", 3, keys, weights, Aggregate::SUM, &union_ret);
    if (!s.ok()) {
        log_info("Test ZInterStore [newkey2, key1, key2, key3] return %s", s.ToString().c_str());
    } else {
        sms.clear();
        s = n->ZRange("tZInterNewKey2", -10, -1, sms);
        log_info(" ZInterStore OK, inter (10 * key1, 1 * key2, 1 * key3) should be [member1=3.5]");
        for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
          log_info("            score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
        }
    }

    weights.clear();
    s = n->ZInterStore("tZInterNewKey3", 2, keys, weights, Aggregate::MAX, &union_ret);
    if (!s.ok()) {
        log_info("Test ZInterStore [newkey3, key1, key2] return %s", s.ToString().c_str());
    } else {
        sms.clear();
        s = n->ZRange("tZInterNewKey3", -10, -1, sms);
        log_info(" ZInterStore OK, inter MAX(1 * key1, 1 * key2) should be [member1=1, member2=2.5, member3=2.9]");
        for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
          log_info("            score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
        }
    }
    log_info("");

    /*
     *  Test SAdd
     */
    int64_t sadd_res;
    log_info("======Test SAdd======");
    s = n->SAdd("setKey", "member1", &sadd_res);
    log_info("Test SAdd OK return %s", s.ToString().c_str());
    log_info("");

    /*
     *  Test Scard
     */
    log_info("======Test SCard======");
    log_info("SCard with existed key return: %ld", n->SCard("setKey"));
    log_info("SCard with non-existe key return: %ld", n->SCard("non-exist"));
    log_info("");

    /*
     *  Test SScan
     */
    log_info("======Test SScan======");
    s = n->SAdd("setKey", "member1", &sadd_res);
    s = n->SAdd("setKey", "member2", &sadd_res);
    s = n->SAdd("setKey", "member3", &sadd_res);

    SIterator *sit = n->SScan("setKey", -1);
    if (sit == NULL) {
        log_info("SScan error!");
    }
    for (; sit->Valid(); sit->Next()) {
        log_info("SScan key: %s, member: %s", sit->key().c_str(), sit->member().c_str()); 
    }
    log_info("");

    /*
     *  Test SMembers
     */
    log_info("======Test SMembers======");
    values.clear();
    s = n->SMembers("setKey", values);
    log_info("Test SMembers return %s, expect [member1, member2, member3]", s.ToString().c_str());
    std::vector<std::string>::iterator smit;
    for (smit = values.begin(); smit != values.end(); smit++) {
        log_info("Test SMembers member: %s", smit->c_str());
    }
    log_info("");

    /*
     *  Test SUnion
     */
    log_info("======Test SUnion======");
    s = n->SAdd("unionKey1", "member1", &sadd_res);
    s = n->SAdd("unionKey1", "member2", &sadd_res);
    s = n->SAdd("unionKey1", "member3", &sadd_res);

    s = n->SAdd("unionKey2", "member21", &sadd_res);
    s = n->SAdd("unionKey2", "member2", &sadd_res);

    keys.clear();
    keys.push_back("unionKey1");
    keys.push_back("unionKey2");
    values.clear();
    s = n->SUnion(keys, values);
    log_info("Test SUnion[key1, key2] return %s, expect [member1, member2, member21, member3]", s.ToString().c_str());
    std::vector<std::string>::iterator suit;
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SUnion member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SUnionStore
     */
    log_info("======Test SUnionStore======");
    s = n->SAdd("unionKey1", "member1", &sadd_res);
    s = n->SAdd("unionKey1", "member2", &sadd_res);
    s = n->SAdd("unionKey1", "member3", &sadd_res);

    s = n->SAdd("unionKey2", "member21", &sadd_res);
    s = n->SAdd("unionKey2", "member2", &sadd_res);

    keys.clear();
    keys.push_back("unionKey1");
    keys.push_back("unionKey2");
    std::string dest = "dest";

    int64_t sus_res;
    s = n->SUnionStore(dest, keys, &sus_res);
    log_info("Test SUnionStore[dest, key1, key2] return %s, card is %ld, expect [member1, member2, member21, member3]", s.ToString().c_str(), sus_res);

    values.clear();
    s = n->SMembers(dest, values);
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SUnionStore member: %s", suit->c_str());
    }
    log_info("");

    s = n->SUnionStore("unionKey1", keys, &sus_res);
    log_info("Test SUnionStore[key1, key1, key2] return %s, card is %ld, expect key1=[member1, member2, member21, member3]", s.ToString().c_str(), sus_res);

    values.clear();
    s = n->SMembers("unionKey1", values);
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SUnionStore member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SInter
     */
    log_info("======Test SInter======");
    s = n->SAdd("interKey1", "member1", &sadd_res);
    s = n->SAdd("interKey1", "member2", &sadd_res);
    s = n->SAdd("interKey1", "member3", &sadd_res);

    s = n->SAdd("interKey2", "member21", &sadd_res);
    s = n->SAdd("interKey2", "member2", &sadd_res);

    keys.clear();
    keys.push_back("interKey1");
    keys.push_back("interKey2");
    values.clear();
    s = n->SInter(keys, values);
    log_info("Test SInter[key1, key2] return %s, expect [member2]", s.ToString().c_str());
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SInter member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SInterStore
     */
    log_info("======Test SInterStore======");
    s = n->SAdd("interKey1", "member1", &sadd_res);
    s = n->SAdd("interKey1", "member2", &sadd_res);
    s = n->SAdd("interKey1", "member3", &sadd_res);

    s = n->SAdd("interKey2", "member21", &sadd_res);
    s = n->SAdd("interKey2", "member2", &sadd_res);

    keys.clear();
    keys.push_back("interKey1");
    keys.push_back("interKey2");
    dest = "dest";

    int64_t sis_res;
    s = n->SInterStore(dest, keys, &sis_res);
    log_info("Test SInterStore[dest, key1, key2] return %s, card is %ld, expect [member2]", s.ToString().c_str(), sis_res);

    values.clear();
    s = n->SMembers(dest, values);
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SInterStore member: %s", suit->c_str());
    }
    log_info("");

    s = n->SInterStore("interKey1", keys, &sis_res);
    log_info("Test SInterStore[key1, key1, key2] return %s, card is %ld, expect key1=[member2]", s.ToString().c_str(), sis_res);

    values.clear();
    s = n->SMembers("interKey1", values);
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SInterStore member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SDiff
     */
    log_info("======Test SDiff======");
    s = n->SAdd("diffKey1", "member1", &sadd_res);
    s = n->SAdd("diffKey1", "member2", &sadd_res);
    s = n->SAdd("diffKey1", "member3", &sadd_res);

    s = n->SAdd("diffKey2", "member21", &sadd_res);
    s = n->SAdd("diffKey2", "member2", &sadd_res);

    keys.clear();
    keys.push_back("diffKey1");
    keys.push_back("diffKey2");
    values.clear();
    s = n->SDiff(keys, values);
    log_info("Test SDiff[key1, key2] return %s, expect [member1, member3]", s.ToString().c_str());
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SDiff member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SDiffStore
     */
    log_info("======Test SDiffStore======");
    s = n->SAdd("diffKey1", "member1", &sadd_res);
    s = n->SAdd("diffKey1", "member2", &sadd_res);
    s = n->SAdd("diffKey1", "member3", &sadd_res);

    s = n->SAdd("diffKey2", "member21", &sadd_res);
    s = n->SAdd("diffKey2", "member2", &sadd_res);

    keys.clear();
    keys.push_back("diffKey1");
    keys.push_back("diffKey2");
    dest = "dest";

    int64_t sds_res;
    s = n->SDiffStore(dest, keys, &sds_res);
    log_info("Test SDiffStore[dest, key1, key2] return %s, card is %ld, expect [member1, member3]", s.ToString().c_str(), sds_res);

    values.clear();
    s = n->SMembers(dest, values);
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SDiffStore member: %s", suit->c_str());
    }
    log_info("");

    s = n->SDiffStore("diffKey1", keys, &sds_res);
    log_info("Test SDiffStore[key1, key1, key2] return %s, card is %ld, expect key1=[member1, member3]", s.ToString().c_str(), sds_res);

    values.clear();
    s = n->SMembers("diffKey1", values);
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SDiffStore member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SRandMember
     */
    log_info("======Test SRandMember======");
    s = n->SAdd("randKey1", "member1", &sadd_res);
    s = n->SAdd("randKey1", "member2", &sadd_res);
    s = n->SAdd("randKey1", "member3", &sadd_res);
    s = n->SAdd("randKey1", "member4", &sadd_res);

    values.clear();
    s = n->SRandMember("randKey1", values, 5);
    log_info("Test SRandMember(5) no-repeated return %s, expect {member[1-4]}", s.ToString().c_str());
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SRandMember member: %s", suit->c_str());
    }
    log_info("");

    values.clear();
    s = n->SRandMember("randKey1", values, -5);
    log_info("Test SRandMember(-5) allow repeated return %s, expect 5 random members", s.ToString().c_str());
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SRandMember member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SPop
     */
    log_info("======Test SPop======");
    s = n->SAdd("popKey1", "member1", &sadd_res);
    s = n->SAdd("popKey1", "member2", &sadd_res);
    s = n->SAdd("popKey1", "member3", &sadd_res);
    s = n->SAdd("popKey1", "member4", &sadd_res);

    values.clear();
    std::string pop_string;
    s = n->SPop("popKey1", pop_string);
    log_info("Test SPop() return %s, pop = (%s), expect random member{member[1-4]}", s.ToString().c_str(), pop_string.c_str());

    values.clear();
    s = n->SMembers("popKey1", values);
    log_info(" After SPop, SMembers return %s, expect 3 members remains", s.ToString().c_str());
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SPop member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SIsMember
     */
    log_info("======Test SIsMember======");
    s = n->SAdd("setKey1", "member1", &sadd_res);
    log_info("Test SIsMember with exist member return %d, expect [true]", n->SIsMember("setKey", "member1"));
    log_info("Test SIsMember with non-exist member return %d, expect [false]", n->SIsMember("setKey", "non-member"));
    log_info("Test SIsMember with non-exist key return %d, expect [false]", n->SIsMember("setKeyasdf", "member"));

    /*
     *  Test SRem
     */
    log_info("======Test SRem======");
    int64_t srem_res;
    s = n->SRem("setKey", "member2", &srem_res);
    log_info("Test SRem with exist member return %s, expect [member1, member3]", s.ToString().c_str());

    values.clear();
    s = n->SMembers("setKey", values);
    for (smit = values.begin(); smit != values.end(); smit++) {
        log_info("    member: %s", smit->c_str());
    }
    log_info("");

    s = n->SRem("setKey", "member2", &srem_res);
    log_info("Test SRem with nonexist member return %s, rem_res is %ld expect [member1, member3]", s.ToString().c_str(), srem_res);
    values.clear();
    s = n->SMembers("setKey", values);
    for (smit = values.begin(); smit != values.end(); smit++) {
        log_info("    member: %s", smit->c_str());
    }
    log_info("");

    /*
     *  Test SMove
     */
    log_info("======Test SMove======");
    s = n->SAdd("moveKey1", "member1", &sadd_res);
    s = n->SAdd("moveKey1", "member2", &sadd_res);
    s = n->SAdd("moveKey2", "member1", &sadd_res);

    int64_t smove_res;
    s = n->SMove("moveKey1", "moveKey1", "member2", &smove_res);
    log_info("Test SMove(key1, key1, member2) return %s, expect key1=%ld [member1 member2] ",
             s.ToString().c_str(), n->SCard("moveKey1"));

    values.clear();
    s = n->SMembers("moveKey1", values);
    for (smit = values.begin(); smit != values.end(); smit++) {
        log_info("    moveKey1 member: %s", smit->c_str());
    }
    values.clear();
    log_info("");

    s = n->SMove("moveKey1", "moveKey2", "member2", &smove_res);
    log_info("Test SMove(key1, key2, member2) return %s, expect key1=%ld [member1]  key2= %ld [member1, member2]",
             s.ToString().c_str(), n->SCard("moveKey1"), n->SCard("moveKey2"));

    values.clear();
    s = n->SMembers("moveKey1", values);
    for (smit = values.begin(); smit != values.end(); smit++) {
        log_info("    moveKey1 member: %s", smit->c_str());
    }
    values.clear();
    s = n->SMembers("moveKey2", values);
    for (smit = values.begin(); smit != values.end(); smit++) {
        log_info("    moveKey2 member: %s", smit->c_str());
    }
    log_info("");


    /*
     *  Test Keys
     */
    log_info("======Test Keys======");
    s = n->Set("setkey", "setval1");
    s = n->HSet("tHSetKey", "tHSetField1", "tSetVal1");
    s = n->LPush("tLPushKey", "tLPushVal1", &llen);
    s = n->ZAdd("tZSetKey", 100.0, "tHMember1", &za_res);
    s = n->SAdd("abc", "member1", &sadd_res);

    keys.clear();
    s = n->Keys("*", keys);
    log_info("Test Keys(\"*\") return %s, size=%ld", s.ToString().c_str(), keys.size());
    int i = 0;
    for (smit = keys.begin(); smit != keys.end(); smit++) {
        log_info(" %d : %s", i++, smit->c_str());
    }

    keys.clear();
    i = 0;
    s = n->Keys("*key*", keys);
    log_info("Test Keys(\"*key*\") return %s, size=%ld", s.ToString().c_str(), keys.size());
    for (smit = keys.begin(); smit != keys.end(); smit++) {
        log_info(" %d :  %s", i++, smit->c_str());
    }

    keys.clear();
    i = 0;
    s = n->Keys("t[HL]Set*", keys);
    log_info("Test Keys(\"t[HL]Set*\") return %s, size=%ld", s.ToString().c_str(), keys.size());
    for (smit = keys.begin(); smit != keys.end(); smit++) {
        log_info(" %d :  %s", i++, smit->c_str());
    }

    /*
     *  Test Exists
     */
    log_info("======Test Exists======");
    s = n->Set("ExistKey", "val1");
    s = n->HSet("HExistKey", "tHSetField1", "tSetVal1");
    s = n->LPush("LExistKey", "tLPushVal1", &llen);
    s = n->ZAdd("ZExistKey", 100.0, "tHMember1", &za_res);
    s = n->SAdd("SExistKey", "member1", &sadd_res);

    keys.clear();
    keys.push_back("ExistKey");
    keys.push_back("HExistKey");
    keys.push_back("LExistKey");
    keys.push_back("ZExistKey");
    keys.push_back("SExistKey");

    s = n->Exists(keys, &llen);
    log_info("Test Exists(5 type) return %s, exists number=%ld", s.ToString().c_str(), llen);

    s = n->Del("ExistKey", &del_ret);
    s = n->Exists(keys, &llen);
    log_info("Test Exists(5 type) after DEL(KV) return %s, exists number=%ld", s.ToString().c_str(), llen);

    s = n->Del("HExistKey", &del_ret);
    s = n->Exists(keys, &llen);
    log_info("Test Exists(5 type) after DEL(KV,HASH) return %s, exists number=%ld", s.ToString().c_str(), llen);

    //s = n->Set("SameExistKey", "val1");
    s = n->HSet("SameExistKey", "tHSetField1", "tSetVal1");
    s = n->LPush("SameExistKey", "tLPushVal1", &llen);

    keys.clear();
    keys.push_back("SameExistKey");
    s = n->Exists(keys, &llen);
    log_info("Test Exists(5 type same key) return %s, exists number=%ld", s.ToString().c_str(), llen);

    /*
     *  Test GetUsage
     */
    log_info("======Test GetUsage======");
    uint64_t usage;
    s = n->GetUsage(USAGE_TYPE_ALL, &usage);
    log_info("Usage all:%lu", usage);
    s = n->GetUsage(USAGE_TYPE_ROCKSDB, &usage);
    log_info("->Usage rocksdb:%lu", usage);
    s = n->GetUsage(USAGE_TYPE_ROCKSDB_MEMTABLE, &usage);
    log_info("->->Usage rocksdb_mem  :%lu", usage);
    s = n->GetUsage(USAGE_TYPE_ROCKSDB_TABLE_READER, &usage);
    log_info("->->Usage rocksdb_table:%lu", usage);
    s = n->GetUsage(USAGE_TYPE_NEMO, &usage);
    log_info("->Usage nemo:   %lu", usage);

    delete n;

    return 0;
}
