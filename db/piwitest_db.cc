/*
 * piwitest_db.cc
 *
 *  Created on: Jul 13, 2016
 *      Author: egilad
 */

#include "piwitest_db.h"
#include <cds/init.h> // for cds::Initialize and cds::Terminate
#include <cds/gc/hp.h> // for cds::HP (Hazard Pointer) SMR
//#include <random>
#include <cstdlib>
#include <iostream>

using std::string;

namespace ycsbc
{

// Initialize Hazard Pointer singleton
constexpr size_t HazPtrCount = 100;
GC hpGC(HazPtrCount);

constexpr double readFileProportion = 0.0; //0.1;
constexpr double readMemProportion = 1.0 - readFileProportion;
static_assert(readFileProportion + readMemProportion == 1);

//std::random_device rd;
//std::mt19937 mt(rd());
//std::uniform_real_distribution<double> dist(0, 1);

const char* fileName = "chunk.data";

PiwiTestDB::PiwiTestDB()
{
	cds::Initialize();
	index = new SkipList();
	fOut.open(fileName);
	if (fOut.bad())
		std::cerr << "Error opening " << fileName << std::endl;
	srand(time(0));
}

PiwiTestDB::~PiwiTestDB()
{
	cds::Terminate();
	delete index;
}

void PiwiTestDB::Init()
{
	// attache to libcds infrastructure
	cds::threading::Manager::attachThread();
}


void PiwiTestDB::Close()
{
	// Detach thread when terminating
	cds::threading::Manager::detachThread();
}

void PiwiTestDB::jumpToMiddle(std::ifstream& fIn)
{
	size_t size = fIn.seekg(0, std::ios_base::end).tellg();
	fIn.seekg(size / 2, std::ios_base::beg);
	string line;
	std::getline(fIn, line); // consume the remain of the current line
}

int PiwiTestDB::DiskRead(const std::string& key, std::vector<KVPair> &result)
{
	std::ifstream fIn(fileName);
	string k, v;
	jumpToMiddle(fIn);
	while (fIn.eof() == false)
	{
		fIn >> k >> v;
		if (fIn.bad())
			break;
		if (k != key)
			continue;
		result.push_back(std::make_pair(k, v));
		return 1;
	}
	return 0;
}

int PiwiTestDB::Read(const std::string &table, const std::string &key,
		const std::vector<std::string> *fields, std::vector<KVPair> &result)
{
//	if (dist(mt) > readMemProportion)
	if (rand() % 100 < 100.0 * readMemProportion)
	{
		auto gp = index->get(key);
		if (gp)
		{
			result.push_back(*gp);
			return 1;
		}
		return 0;
	}
	return DiskRead(key, result);
}

int PiwiTestDB::Scan(const std::string &table, const std::string &key, int record_count,
		const std::vector<std::string> *fields,
		std::vector<std::vector<KVPair>> &result)
{
	return 0;
}

void PiwiTestDB::DiskWrite(const string& key, const string& value)
{
//	string line = key + "\t" + value + "\n";
//	std::lock_guard<std::mutex> guard(fWriteLock); // lock actual write to avoid corruption
//	fOut << line << std::flush;
}

int PiwiTestDB::Update(const std::string &table, const std::string &key,
		std::vector<KVPair> &values)
{
	DiskWrite(key, values[0].second);
	return index->emplace(key, values[0].second) == true;
}

int PiwiTestDB::Insert(const std::string &table, const std::string &key,
		std::vector<KVPair> &values)
{
	DiskWrite(key, values[0].second);
	return index->insert(key, values[0].second);
}

int PiwiTestDB::Delete(const std::string &table, const std::string &key)
{
	DiskWrite(key, "_|_");
	return index->erase(key);
}

} /* namespace ycsbc */
