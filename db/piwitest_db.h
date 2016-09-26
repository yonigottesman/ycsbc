/*
 * piwitest_db.h
 *
 *  Created on: Jul 13, 2016
 *      Author: egilad
 */

#ifndef DB_PIWITEST_DB_H_
#define DB_PIWITEST_DB_H_

#include "core/db.h"
#include <cds/container/skip_list_map_dhp.h>
#include <string>
#include <fstream>
#include <mutex>

namespace ycsbc
{

typedef cds::gc::HP GC;

class PiwiTestDB: public DB
{
	typedef std::string Key;
	typedef std::string Value;
	typedef cds::container::SkipListMap<GC, Key, Value> SkipList;

	SkipList* index;
	std::ofstream fOut;
	std::mutex fWriteLock;

	int DiskRead(const std::string& key, std::vector<KVPair> &result);
	void DiskWrite(const std::string& key, const std::string& value);
	void jumpToMiddle(std::ifstream& fIn);

public:
	PiwiTestDB();
	virtual ~PiwiTestDB();
	///
	/// Initializes any state for accessing this DB.
	/// Called once per DB client (thread); there is a single DB instance globally.
	///
	void Init() override;
	///
	/// Clears any state for accessing this DB.
	/// Called once per DB client (thread); there is a single DB instance globally.
	///
	void Close() override;
	///
	/// Reads a record from the database.
	/// Field/value pairs from the result are stored in a vector.
	///
	/// @param table The name of the table.
	/// @param key The key of the record to read.
	/// @param fields The list of fields to read, or NULL for all of them.
	/// @param result A vector of field/value pairs for the result.
	/// @return Zero on success, or a non-zero error code on error/record-miss.
	///
	int Read(const std::string &table, const std::string &key,
			const std::vector<std::string> *fields, std::vector<KVPair> &result)
					override;
	///
	/// Performs a range scan for a set of records in the database.
	/// Field/value pairs from the result are stored in a vector.
	///
	/// @param table The name of the table.
	/// @param key The key of the first record to read.
	/// @param record_count The number of records to read.
	/// @param fields The list of fields to read, or NULL for all of them.
	/// @param result A vector of vector, where each vector contains field/value
	///        pairs for one record
	/// @return Zero on success, or a non-zero error code on error.
	///
	int Scan(const std::string &table, const std::string &key, int record_count,
			const std::vector<std::string> *fields,
			std::vector<std::vector<KVPair>> &result) override;
	///
	/// Updates a record in the database.
	/// Field/value pairs in the specified vector are written to the record,
	/// overwriting any existing values with the same field names.
	///
	/// @param table The name of the table.
	/// @param key The key of the record to write.
	/// @param values A vector of field/value pairs to update in the record.
	/// @return Zero on success, a non-zero error code on error.
	///
	int Update(const std::string &table, const std::string &key,
			std::vector<KVPair> &values) override;
	///
	/// Inserts a record into the database.
	/// Field/value pairs in the specified vector are written into the record.
	///
	/// @param table The name of the table.
	/// @param key The key of the record to insert.
	/// @param values A vector of field/value pairs to insert in the record.
	/// @return Zero on success, a non-zero error code on error.
	///
	int Insert(const std::string &table, const std::string &key,
			std::vector<KVPair> &values) override;
	///
	/// Deletes a record from the database.
	///
	/// @param table The name of the table.
	/// @param key The key of the record to delete.
	/// @return Zero on success, a non-zero error code on error.
	///
	int Delete(const std::string &table, const std::string &key) override;
};

} /* namespace ycsbc */

#endif /* DB_PIWITEST_DB_H_ */
