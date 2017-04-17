//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include "db.h"
#include "core_workload.h"
#include "utils.h"
#include "statistics.h"
#include "histogram_accumulator.h"

namespace ycsbc {

class Client
{
public:
	Client(DB &db, CoreWorkload &wl, size_t opsNum) :
			db_(db), workload_(wl),
			stats(OPS_NUM, Statistics(opsNum)), scanStats(opsNum),
			histograms(OPS_NUM, HistogramAccumulator(0.0, 1.0, 40))
			// note: opsNum refer to all ops, while stats are gathered separately.
			// depending on the mix, the actual number of events passed to a stats
			// object can be much smaller than the declared.
	{
	}

	virtual bool DoInsert();
	virtual bool DoTransaction();

	virtual ~Client()
	{
	}

	const Statistics& getStats(Operation op) const {
		return stats[op];
	}
	const Statistics& getScanStats() const {
		return scanStats;
	}
	const HistogramAccumulator& getHistogram(Operation op) const {
	    return histograms[op];
	}
    size_t getBytesRead() const {
        return bytesRead;
    }
    size_t getBytesWritten() const {
        return bytesWritten;
    }

protected:

	virtual int TransactionRead();
	virtual int TransactionReadModifyWrite();
	virtual int TransactionScan();
	virtual int TransactionUpdate();
	virtual int TransactionInsert();

	DB &db_;
	CoreWorkload &workload_;
	std::vector<Statistics> stats;
	Statistics scanStats;
	std::vector<HistogramAccumulator> histograms;
	size_t bytesRead = 0, bytesWritten = 0;
	utils::Timer<double> timer; // ok since each client is used by a single thread

private:
    void placeKeyInValue(std::vector<DB::KVPair>& values,
            const std::string& key) const;
    bool checkKeyInValue(const std::vector<DB::KVPair>& result,
            const std::string& key) const;
};

inline bool Client::DoInsert() {
  std::string key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> pairs;
  workload_.BuildValues(pairs);
  bytesWritten += key.size() + pairs[0].second.size();
  return (db_.Insert(workload_.NextTable(), key, pairs) == DB::kOK);
}

inline bool Client::DoTransaction() {
  int status = -1;
  timer.Start();
  Operation op = workload_.NextOperation();
  switch (op) {
    case READ:
      status = TransactionRead();
      break;
    case UPDATE:
      status = TransactionUpdate();
      break;
    case INSERT:
      status = TransactionInsert();
      break;
    case SCAN:
      status = TransactionScan();
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite();
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  double dur = timer.End() * 1000;
  stats[op].addEvent(dur);
  histograms[op].addVal(dur);
  assert(status >= 0);
  return (status == DB::kOK);
}

inline int Client::TransactionRead() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;
  std::vector<std::string> fields, *readFields = nullptr;
  if (!workload_.read_all_fields()) {
    fields.push_back("field" + workload_.NextFieldName());
    readFields = &fields;
  }
  int ret = db_.Read(table, key, readFields, result);
  if (!result.empty())
  {
    bytesRead += result[0].second.size();
    if (workload_.validateGets())
    {
        bool ok = checkKeyInValue(result, key);
        if (!ok)
        {
            std::cerr << "Read of key " << key << " returned wrong value " << result[0].second << std::endl;
            return DB::kErrorNoData;
        }
    }
  }
  return ret;
}

inline int Client::TransactionReadModifyWrite() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;

  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    db_.Read(table, key, &fields, result);
  } else {
    db_.Read(table, key, NULL, result);
  }

  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values);
}

inline int Client::TransactionScan() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  int len = workload_.NextScanLength();
  std::vector<std::vector<DB::KVPair>> result;
  int ret;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    ret = db_.Scan(table, key, len, &fields, result);
  } else {
    ret = db_.Scan(table, key, len, NULL, result);
  }
  scanStats.addEvent(result.size());
  for (const auto& vec : result)
      bytesRead += vec[0].second.size();
  return ret;
}

inline int Client::TransactionUpdate() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  if (workload_.validateGets())
    placeKeyInValue(values, key);
  bytesWritten += key.size() + values[0].second.size();
  return db_.Update(table, key, values);
}

inline int Client::TransactionInsert() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> values;
  workload_.BuildValues(values);
  if (workload_.validateGets())
    placeKeyInValue(values, key);
  bytesWritten += key.size() + values[0].second.size();
  return db_.Insert(table, key, values);
} 

inline void Client::placeKeyInValue(std::vector<DB::KVPair>& values,
        const std::string& key) const
{
    std::string& value = values[0].second;
    size_t copylen = std::max(key.size(), value.size());
    std::copy(key.c_str(), key.c_str() + copylen, &value[0]);
}

bool Client::checkKeyInValue(const std::vector<DB::KVPair>& result,
        const std::string& key) const
{
    const std::string& value = result[0].second;
    size_t complen = std::max(key.size(), value.size());
    bool ok = std::strncmp(key.c_str(), value.c_str(), complen) == 0;
    return ok;
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
