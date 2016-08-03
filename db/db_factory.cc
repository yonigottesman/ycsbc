//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db/db_factory.h"

#include <string>
#include "db/basic_db.h"
#include "db/lock_stl_db.h"
#include "db/tbb_rand_db.h"
#include "db/tbb_scan_db.h"
#include "db/piwitest_db.h"
#include "db/piwi_db.h"
#include "db/rocks_db.h"

using namespace std;

namespace ycsbc
{

DB* DBFactory::CreateDB(utils::Properties &props) {
  if (props["dbname"] == "basic") {
    return new BasicDB;
  } else if (props["dbname"] == "lock_stl") {
    return new LockStlDB;
//  } else if (props["dbname"] == "tbb_rand") {
//    return new TbbRandDB;
//  } else if (props["dbname"] == "tbb_scan") {
//    return new TbbScanDB;
  } else if (props["dbname"] == "piwi_test") {
    return new PiwiTestDB;
  } else if (props["dbname"] == "piwi") {
    return new PiwiDB(props.properties());
  } else if (props["dbname"] == "rocks") {
    return new RocksDB;
  } else return NULL;
}

}
