/*
 * rocks_db.cpp
 *
 *  Created on: Aug 3, 2016
 *      Author: egilad
 */

#include "rocks_db.h"

#include "rocksdb/slice.h"
#include "rocksdb/options.h"


using namespace std;

namespace ycsbc
{

namespace
{
string kDBPath = "data_rocksdb";
}

RocksDB::RocksDB()
{
	rocksdb::Options options;
	// Optimize RocksDB. This is the easiest way to get RocksDB to perform well
	options.IncreaseParallelism();
	options.OptimizeLevelStyleCompaction();
	// create the DB if it's not already present
	options.create_if_missing = true;

	// open DB
	rocksdb::Status s = rocksdb::DB::Open(options, kDBPath, &db);
	assert(s.ok());
}

RocksDB::~RocksDB()
{
	delete db;
}

int RocksDB::Insert(const string &table, const string &key,
		vector<KVPair> &values)
{
	// Put key-value
	rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, values[0].second);
	if (!s.ok())
		return DB::kErrorConflict;
	return DB::kOK;
}

int RocksDB::Read(const string &table, const string &key,
		const vector<string> *fields, vector<KVPair> &result)
{
	string value;
	rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &value);
	if (!s.ok())
		return DB::kErrorNoData;
	result.push_back(KVPair("", value));
	return DB::kOK;
}

} /* namespace ycsbc */
