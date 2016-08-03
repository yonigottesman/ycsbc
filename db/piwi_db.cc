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

} /* namespace ycsbc */
