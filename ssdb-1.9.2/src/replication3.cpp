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
    log_info("ReplicationByIterator2::process");


    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    Link *master_link = upstream;








    return 0;
}