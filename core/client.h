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

namespace ycsbc {

class Client
{
public:
	Client(DB &db, CoreWorkload &wl, size_t opsNum) :
			db_(db), workload_(wl),
			stats(OPS_NUM, Statistics(opsNum)), scanStats(opsNum)
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
	size_t bytesRead = 0, bytesWritten = 0;
	utils::Timer<double> timer; // ok since each client is used by a single thread
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
  stats[op].addEvent(timer.End() * 1000);
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
      bytesRead += result[0].second.size();
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
  bytesWritten += key.size() + values[0].second.size();
  return db_.Update(table, key, values);
}

inline int Client::TransactionInsert() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> values;
  workload_.BuildValues(values);
  bytesWritten += key.size() + values[0].second.size();
  return db_.Insert(table, key, values);
} 

} // ycsbc

#endif // YCSB_C_CLIENT_H_
