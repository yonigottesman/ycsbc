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
//	size_t magnification = 9;
//	size_t modDigits = 9 + magnification; // first 9: digits in int
//	const char* fromNum = from.c_str() + from.size() - modDigits;
//	size_t num = stoll(fromNum);
//	num += (size_t)record_count * pow(10, (magnification));
	size_t modDigits = 9; // 9 digits in int
	const char* fromNum = from.c_str() + from.size() - modDigits;
	size_t num = stoll(fromNum);
	num += (size_t)record_count;
	stringstream ss;
	ss << from.substr(0, from.size() - modDigits) << setfill('0') << setw(modDigits) << to_string(num);
	string to = ss.str().substr(0, from.size());
	list<KVPair> resultList;
	piwi::scan(from, to, resultList);
//	clog << "scan for " << from << " to " << to << " (" << setw(9) << record_count <<
//			") items returned " << resultList.size() <<
//			" results [" << __FILE__ << ":" << __LINE__ << "]" << endl; //XXX
	// avoiding the copy from list to vector for now - it's pointless and wasteful
	result.resize(1);
	result[0].emplace_back(from, to);
	return DB::kOK;
}


} /* namespace ycsbc */
