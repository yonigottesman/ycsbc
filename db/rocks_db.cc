/*
 * rocks_db.cpp
 *
 *  Created on: Aug 3, 2016
 *      Author: egilad
 */

#include "rocks_db.h"

#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <port/port_posix.h>
#include <table/block_based_table_factory.h>

#include <algorithm>
#include <cctype>

using namespace std;

namespace ycsbc
{

namespace
{
string DBPath = "data_rocksdb";

const string UseFsync = "rocksdb_usefsync";
const string SyncWrites = "rocksdb_syncwrites";
const string CacheSize = "rocksdb_cachesize";
const string Compression = "compression";
const string WriteBufferSize = "write_buffer_size";
const string MaxWriteBufferNumber = "max_write_buffer_number";
const string MinWriteBufferToMerge = "min_write_buffer_number_to_merge";
const string statistics = "statistics";
const string StatsDumpPeriod = "stats_dump_period_sec";

const size_t defaultWriteBufferSize = 256*1024*1024;
const int defaultMaxWriteBufferNumber = 5;
const int defaultMinWriteBufferToMerge = 2;
const std::shared_ptr<rocksdb::Statistics> defaultStatistics = nullptr;
const unsigned int defaultStatsDumpTime = 3600; //one hour

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

size_t writeBufferSize(const map<string, string>& props)
{
    auto iter = props.find(WriteBufferSize);
    if (iter == props.end())
        return defaultWriteBufferSize;
    return stoll(iter->second);
}

int maxWriteBufferNumber(const map<string, string>& props)
{
    auto iter = props.find(MaxWriteBufferNumber);
    if (iter == props.end())
        return defaultMaxWriteBufferNumber;
    return stoi(iter->second);
}

int minWriteBufferToMerge(const map<string, string>& props)
{
    auto iter = props.find(MinWriteBufferToMerge);
    if (iter == props.end())
        return defaultMinWriteBufferToMerge;
    return stoi(iter->second);
}

unsigned int statsDumpPeriod(const map<string, string>& props)
{
    auto iter = props.find(StatsDumpPeriod);
    if (iter == props.end())
        return defaultStatsDumpTime;
    return stoul(iter->second);
}


void setCacheBlockSize(const map<string, string>& props, rocksdb::Options& options)
{
    size_t capacity = blockCacheSize(props);
    if (capacity == 0)
        return;
    shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(capacity);
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = cache;
    options.table_factory.reset(new rocksdb::BlockBasedTableFactory(table_options));
}

void setStatistics(const map<string, string>& props, rocksdb::Options& options)
{
    auto iter = props.find(statistics);
    if (iter == props.end()){
        options.statistics = defaultStatistics;
    }
    else{
    		string stat = iter->second;
    		transform(stat.begin(), stat.end(), stat.begin(), [](char c) { return tolower(c); });
    		if (stat == "off" || stat == "no" || stat == "false" ||  stat == "0"){
    			options.statistics = defaultStatistics;
    		}
    		if (stat == "on" || stat == "yes" || stat == "true" || stat == "1"){
    			options.statistics = rocksdb::CreateDBStatistics();
    		}
    }
  //  return options.statistics;
}

rocksdb::CompressionType doCompress(const map<string, string>& props,
        rocksdb::CompressionType defaultCompress)
{
    auto iter = props.find(Compression);
    if (iter == props.end())
        return defaultCompress;
    string comp = iter->second;
    transform(comp.begin(), comp.end(), comp.begin(), [](char c) { return tolower(c); });
    if (comp == "off" || comp == "no" || comp == "false" || comp == "0")
        return rocksdb::kNoCompression;
    if (comp == "on" || comp == "yes" || comp == "true" || comp == "1")
        return rocksdb::kSnappyCompression;
    return defaultCompress;
}

void setCompression(const map<string, string>& props, rocksdb::Options& options)
{
    constexpr rocksdb::CompressionType defaultCompression =
            rocksdb::kNoCompression;
    auto compression = doCompress(props, defaultCompression);
    options.compression = compression;
    fill(options.compression_per_level.begin(), options.compression_per_level.end(), compression);
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
    setCompression(props, options);

    options.write_buffer_size = writeBufferSize(props);
    options.max_write_buffer_number = maxWriteBufferNumber(props);
    options.min_write_buffer_number_to_merge = minWriteBufferToMerge(props);
    setStatistics(props, options);
    options.stats_dump_period_sec = statsDumpPeriod(props);

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
	rocksdb::Options options = db->GetOptions();
	if (options.statistics != nullptr){
		std::cout << std::endl << "RocksDB Statistics:" << std::endl;
		std::cout << options.statistics->ToString();
		std::cout << std::endl << "Histogram DB Gets:" <<std::endl;
		std::cout << options.statistics->getHistogramString(rocksdb::DB_GET) << std::endl;
		std::cout << std::endl << "Histogram DB Writes:" <<std::endl;
		std::cout << options.statistics->getHistogramString(rocksdb::DB_WRITE) << std::endl;
		std::cout << std::endl << "Histogram Compaction Time:" <<std::endl;
		std::cout << options.statistics->getHistogramString(rocksdb::COMPACTION_TIME) << std::endl;
		std::cout << std::endl << "Histogram Compaction Outfile Sync Micros" << std::endl;
		std::cout << options.statistics->getHistogramString(rocksdb::COMPACTION_OUTFILE_SYNC_MICROS) << std::endl;
		std::cout << std::endl << "Histogram Stall Memtable Compaction Count:" << std::endl;
		std::cout << options.statistics->getHistogramString(rocksdb::STALL_MEMTABLE_COMPACTION_COUNT) << std::endl;

	}
	else {
		std::cout << "Statistics printout is off" << std::endl;
	}
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
