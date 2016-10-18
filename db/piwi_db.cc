/*
 * piwi_db.cc
 *
 *  Created on: Jul 31, 2016
 *      Author: egilad
 */

#include "piwi_db.h"
#include <libpiwi.h>

#include <iostream>
#include <iomanip>
#include <sstream>
#include <cmath>

using namespace std;

namespace ycsbc
{

PiwiDB::PiwiDB(const map<string, string>& props)
{
	piwi::init(props);
}

PiwiDB::~PiwiDB()
{
	piwi::shutdown();
}

int PiwiDB::Insert(const string& table, const string& key,
		vector<KVPair>& values)
{
	piwi::put(key, values[0].second);
	return DB::kOK;
}

int PiwiDB::Read(const string &table, const string &key,
			const vector<string> *fields, vector<KVPair> &result)
{
	string value;
	piwi::get(key, value);
	result.push_back(KVPair("", value));
	return DB::kOK;
}

int PiwiDB::Scan(const string &table, const string &from, int record_count,
		const vector<string> *fields,
		vector<vector<KVPair>> &result)
{
	result.reserve(record_count);
	string to = getScanTo(from, record_count);
	list<KVPair> resultList;
	piwi::scan(from, to, resultList);
	for (const KVPair kv : resultList)
		result.emplace_back(vector<KVPair>{kv});
	return DB::kOK;
}


} /* namespace ycsbc */
