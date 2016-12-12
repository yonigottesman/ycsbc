/*
 * multi_rocks_db.cpp
 *
 *  Created on: Dec 11, 2016
 *      Author: egilad
 */

#include "multi_rocks_db.h"

#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include <sstream>
#include <functional> // for hash

using namespace std;

namespace ycsbc
{

namespace
{
string DBPath = "data_rocksdb";

const string UseFsync = "rocksdb_usefsync";
const string SyncWrites = "rocksdb_syncwrites";
const string Shards = "rocksdb_shards";

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

size_t shardsNum(const map<string, string>& props)
{
    auto iter = props.find(Shards);
    if (iter == props.end())
        return 1;
    stringstream ss(iter->second);
    size_t ret;
    ss >> ret;
    return ret;
}

size_t getShard(const string& key, size_t shardNum)
{
    size_t h = hash<string>{}(key);
    return h % shardNum;
}

} // namespace

MultiRocksDB::MultiRocksDB(const map<string, string>& props, const string& dbDir)
{
    rocksdb::Options options;
	// Optimize MultiRocksDB. This is the easiest way to get MultiRocksDB to perform well
	options.IncreaseParallelism();
	options.OptimizeLevelStyleCompaction();
	// enable parallel writes
	options.allow_concurrent_memtable_write = true;
	options.enable_write_thread_adaptive_yield = true;
	// create the DB if it's not already present
	options.create_if_missing = true;

	options.use_fsync = useFsync(props);
	wo.sync = syncWrites(props);

	size_t shards = shardsNum(props);
	dbs.resize(shards);
    stringstream ss;
	for (size_t i = 0; i < shards; ++i)
	{
	    ss.str("");
	    ss << dbDir << DBPath << "/" << i;
        // open DB
	    verifyDirExists(ss.str());
	    rocksdb::Status s = rocksdb::DB::Open(options, ss.str(), &dbs[i]);
        if (!s.ok())
        {
            cerr << "MultiRocksDB DB #" << i << " could not be opened: " << s.ToString() << endl;
            assert(false);
        }
	}
}

MultiRocksDB::~MultiRocksDB()
{
    for (auto db : dbs)
        delete db;
}

int MultiRocksDB::Insert(const string &table, const string &key,
		vector<KVPair> &values)
{
    rocksdb::DB* db = dbs[getShard(key, dbs.size())];
	// Put key-value
    rocksdb::Status s = db->Put(wo, key, values[0].second);
	if (!s.ok())
		return kErrorConflict;
	return kOK;
}

int MultiRocksDB::Read(const string &table, const string &key,
		const vector<string> *fields, vector<KVPair> &result)
{
    rocksdb::DB* db = dbs[getShard(key, dbs.size())];
	string value;
	rocksdb::Status s = db->Get(ro, key, &value);
	if (!s.ok())
		return kErrorNoData;
	result.push_back(KVPair("", value));
	return kOK;
}

int MultiRocksDB::Scan(const string &table, const string &key, int record_count,
		const vector<string> *fields,
		vector<vector<KVPair>>& result)
{
	result.reserve(record_count);
	for (auto db : dbs)
	{
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
	}
	return kOK;
}


} /* namespace ycsbc */
