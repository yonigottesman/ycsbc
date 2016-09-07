/*
 * piwi_db.cc
 *
 *  Created on: Jul 31, 2016
 *      Author: egilad
 */

#include "piwi_db.h"
#include <libpiwi.h>

#include <iostream>

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
	const char* fromNum = from.c_str() + from.size() - 9; // 9 = digits in int
	int num = stoi(fromNum);
	num += record_count;
	string to = from.substr(0, from.size() - 9);
	to += to_string(num);
	list<KVPair> resultList;
	piwi::scan(from, to, resultList);
	// avoiding the copy from list to vector for now - it's pointless and wasteful
	result[0].emplace_back(from, to);
	return DB::kOK;
}


} /* namespace ycsbc */
