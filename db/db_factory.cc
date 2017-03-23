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
#include "db/piwi_db.h"
#include "db/rocks_db.h"
#include "db/multi_rocks_db.h"
#include "db/stats_db.h"

using namespace std;

namespace ycsbc
{

namespace
{
string getDbDir(const map<string, string>& props)
{
    auto iter = props.find("dbdir");
    string dir = iter == props.end() ? "./" : iter->second;
    if (dir.empty())
        dir = "./";
    if (*dir.rbegin() != '/')
        dir += '/';
    return dir;
}
}

DB* DBFactory::CreateDB(utils::Properties &props) {
  if (props["dbname"] == "basic") {
    return new BasicDB;
  } else if (props["dbname"] == "lock_stl") {
    return new LockStlDB;
  } else if (props["dbname"] == "piwi") {
    return new PiwiDB(props.properties(), getDbDir(props.properties()));
  } else if (props["dbname"] == "rocks") {
    return new RocksDB(props.properties(), getDbDir(props.properties()));
  } else if (props["dbname"] == "multi_rocks") {
    return new MultiRocksDB(props.properties(), getDbDir(props.properties()));
  } else if (props["dbname"] == "stats") {
    return new StatsDb(props.properties());
  } else return NULL;
}

}
