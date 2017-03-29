/*
 * rocks_db.cpp
 *
 *  Created on: Aug 3, 2016
 *      Author: egilad
 */

#include "rocks_db.h"

#include "rocksdb/slice.h"
#include "rocksdb/options.h"
//#include <rocksdb/table/block_based_table_factory.h>

using namespace std;

namespace ycsbc
{

namespace
{
string DBPath = "data_rocksdb";

const string UseFsync = "rocksdb_usefsync";
const string SyncWrites = "rocksdb_syncwrites";
const string CacheSize = "rocksdb_cachesize";

bool useFsync(const map<string, string>& props)
{
    auto iter = props.find(UseFsync);
    if (iter == props.end())
        return false;
    return settingToBool(iter->second);
}

bool syncWrites(const map<string, string>& props)
{
    auto iter = props.find(SyncWrites);
    if (iter == props.end())
        return false;
    return settingToBool(iter->second);
}

size_t blockCacheSize(const map<string, string>& props)
{
    auto iter = props.find(CacheSize);
    if (iter == props.end())
        return 0;
    return stoll(iter->second);
}

void setCacheBlockSize(const map<string, string>& props, rocksdb::Options& options)
{
//    size_t capacity = blockCacheSize(props);
//    if (capacity == 0)
//        return;
//    shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(capacity);
//    rocksdb::BlockBasedTableOptions table_options;
//    table_options.block_cache = cache;
//    options.table_factory.reset(new rocksdb::BlockBasedTableFactory(table_options));
}
} // namespace

RocksDB::RocksDB(const map<string, string>& props, const string& dbDir)
{
	rocksdb::Options options;
	// Optimize RocksDB. This is the easiest way to get RocksDB to perform well
	options.IncreaseParallelism();
	options.OptimizeLevelStyleCompaction();
	// enable parallel writes
	options.allow_concurrent_memtable_write = true;
	options.enable_write_thread_adaptive_yield = true;
	// create the DB if it's not already present
	options.create_if_missing = true;

	options.use_fsync = useFsync(props);
	wo.sync = syncWrites(props);

	setCacheBlockSize(props, options);

	// open DB
	verifyDirExists(dbDir + DBPath);
	rocksdb::Status s = rocksdb::DB::Open(options, dbDir + DBPath, &db);
	if (!s.ok())
	{
		cerr << "RocksDB DB could not be opened: " << s.ToString() << endl;
		assert(false);
	}
}

RocksDB::~RocksDB()
{
	delete db;
}

int RocksDB::Insert(const string &table, const string &key,
		vector<KVPair> &values)
{
	// Put key-value
	rocksdb::Status s = db->Put(wo, key, values[0].second);
	if (!s.ok())
		return DB::kErrorConflict;
	return DB::kOK;
}

int RocksDB::Read(const string &table, const string &key,
		const vector<string> *fields, vector<KVPair> &result)
{
	string value;
	rocksdb::Status s = db->Get(ro, key, &value);
	if (!s.ok())
		return DB::kErrorNoData;
	result.push_back(KVPair("", value));
	return DB::kOK;
}

int RocksDB::Scan(const string &table, const string &key, int record_count,
		const vector<string> *fields,
		vector<vector<KVPair>>& result)
{
	result.reserve(record_count);
	rocksdb::ReadOptions readOps;
	rocksdb::ManagedSnapshot raiiSnap(db);
	readOps.snapshot = raiiSnap.snapshot();
	rocksdb::Iterator* iter = db->NewIterator(readOps);
	rocksdb::Slice start(key);
	string end = getScanTo(key, record_count);

	for (iter->Seek(start); iter->Valid(); iter->Next())
	{
		string key(iter->key().ToString());
		string val(iter->value().ToString());
		if (key > end)
			break;
		result.emplace_back(vector<KVPair>{KVPair{key, val}});
		if (result.size() == (size_t)record_count)
			break;
	}
	delete iter;
	return DB::kOK;
}

int RocksDB::Update(const string &table, const string &key,
        vector<KVPair> &values)
{
    return Insert(table, key, values);
}


} /* namespace ycsbc */
