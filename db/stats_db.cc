/*
 * stats_db.cc
 *
 *  Created on: Mar 22, 2017
 *      Author: erangi
 */

#include "stats_db.h"

#include <algorithm>
#include <numeric>
#include <iostream>
#include <fstream>
#include <iomanip>

using namespace std;

namespace ycsbc
{

struct KeyCnt
{
    size_t count;
    KeyCnt(const pair<const string&, size_t>& mapElem) : count(mapElem.second) {}
    KeyCnt& operator=(const pair<const string&, size_t>& mapElem) {
        count = mapElem.second;
        return *this;
    }
};

void StatsDb::calcMunkKeys(const map<string, string>& props,
        size_t& munkKeys, size_t& munkBytesCapacity, size_t& valSize)
{
    string val;
    size_t keySize = 0, munkEntrySize;
    munkBytesCapacity = valSize = 0;
    val = props.at("munkBytesCapacity");
    if (!val.empty())
        munkBytesCapacity = stoll(val);

    val = props.at("exact_key_size");
    if (!val.empty())
        keySize = stoll(val);

    val = props.at("fieldlength");
    if (!val.empty())
        valSize = stoll(val);

    if (munkBytesCapacity * keySize == 0)
    {
        cerr << "munkBytesCapacity and/or exact_key_size missing, can't calc munk size!" << endl;
        munkKeys = 1;
        return;
    }
    munkEntrySize = sizeof(const char*) + sizeof(std::fstream::off_type)
            + sizeof(int32_t) + sizeof(uint16_t) + keySize - sizeof("user");
    munkKeys = munkBytesCapacity / munkEntrySize;
}

StatsDb::StatsDb(const map<string, string>& props)
{
    calcMunkKeys(props, const_cast<size_t&>(givenMunkKeys),
            const_cast<size_t&>(munkBytesCapacity), const_cast<size_t&>(valSize));
}

StatsDb::~StatsDb()
{
    clog << "Configuration: munk size " << munkBytesCapacity << "B, " << givenMunkKeys <<
            " keys per munk, " << valSize << "B values." << endl;
    clog << "\nKey-level distribution (considering 1 key per chunk):" << endl;
    printStats(1);
    clog << "\nKey-level distribution given specified munk size (" << munkBytesCapacity << "B):" << endl;
    printStats(givenMunkKeys);
    clog << "\nKey-level distribution with munks 10 time smaller:" << endl;
    printStats(givenMunkKeys / 10);
    clog << "\nKey-level distribution with munks 10 time larger:" << endl;
    printStats(givenMunkKeys * 10);
}

void StatsDb::printStats(size_t munkKeys)
{
    vector<size_t> sorted;
    sorted.reserve(keyCounters.size() / munkKeys);
    size_t chunkCtr = 0, keyCtr = 0;
    for (auto kc : keyCounters)
    {
        chunkCtr += kc.second;
        ++keyCtr;
        if (keyCtr == munkKeys)
        {
            sorted.emplace_back(chunkCtr);
            chunkCtr = keyCtr = 0;
        }
    }
    if (keyCtr > 0)
        sorted.emplace_back(chunkCtr);
    sort(sorted.begin(), sorted.end(), [](const size_t& kc1, const size_t& kc2) {
        return kc2 < kc1;
    });
    size_t totalCnts = accumulate(sorted.begin(), sorted.end(), 0, [](size_t total, size_t kc) {
        return total + kc;
    });
    const double range = (double)totalCnts / 100;
    size_t percentile = 10;
    size_t partTotal = 0;
    clog << totalCnts << " ops run on " << sorted.size() << " chunks, " << munkKeys <<
            " keys per munk" << endl;
    clog << "max ops per chunk: " << *sorted.begin() <<
            ", min ops per chunk: " << *sorted.rbegin() << ", " <<
            sorted.size() - distance(
                    sorted.begin(),
                    find_if(sorted.begin(), sorted.end(),
                            [](size_t keyCnt) { return keyCnt == 0;})) <<
            " chunk with no ops" << endl;
    size_t percentile90 = 0;
    for (size_t i = 0; i < sorted.size(); ++i)
    {
        partTotal += sorted[i];
        if (percentile * range <= partTotal)
        {
            clog << setw(3) << percentile << "% of ops obtained by " << setw(8) <<
                    (i <= 1 ? 1 : i - 1) << " chunks (" << fixed << setprecision(1) << 100.0 * i / sorted.size() << "% of all chunks)" << endl;
            if (percentile == 90)
                percentile90 = i - 1;
            percentile += 10;
        }
    }
    size_t entrySize = munkBytesCapacity / givenMunkKeys;
    size_t totalRam = percentile90 * munkKeys * (entrySize + valSize);
    clog << "Memory footprint for " << percentile90 << " full munks, including values: " << totalRam << endl;
}

int StatsDb::Insert(const std::string &table, const std::string &key,
        std::vector<KVPair> &values)
{
    keyCounters[key] = 0;
    return DB::kOK;
}

int StatsDb::Read(const std::string &table, const std::string &key,
        const std::vector<std::string> *fields, std::vector<KVPair> &result)
{
    ++keyCounters[key];
    return DB::kOK;
}

int StatsDb::Update(const std::string &table, const std::string &key,
        std::vector<KVPair> &values)
{
    ++keyCounters[key];
    return DB::kOK;
}

int StatsDb::Scan(const std::string &table, const std::string &key, int record_count,
        const std::vector<std::string> *fields,
        std::vector<std::vector<KVPair>> &result)
{
    auto iter = keyCounters.find(key);
    for (int c = 0; c < record_count && iter != keyCounters.end(); ++c, ++iter)
        ++iter->second;
    return DB::kOK;
}

} /* namespace ycsbc */
