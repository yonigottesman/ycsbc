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

StatsDb::StatsDb(const map<string, string>& props)
{
    string val;
    val = props[]
}

StatsDb::~StatsDb()
{
    printStats(1);
    printStats(100);
    printStats(1000);
    printStats(10000);
}

void StatsDb::printStats(size_t chunkSize)
{
    clog << "Transforming stats for chunk size " << chunkSize << " keys..." << endl;
    vector<size_t> sorted;
    sorted.reserve(keyCounters.size() / chunkSize);
    size_t chunkCtr = 0, keyCtr = 0;
    for (auto kc : keyCounters)
    {
        chunkCtr += kc.second;
        ++keyCtr;
        if (keyCtr == chunkSize)
        {
            sorted.emplace_back(chunkCtr);
            chunkCtr = keyCtr = 0;
        }
    }
    if (keyCtr > 0)
        sorted.emplace_back(chunkCtr);
    clog << "Sorting results..." << endl;
    sort(sorted.begin(), sorted.end(), [](const size_t& kc1, const size_t& kc2) {
        return kc2 < kc1;
    });
    size_t totalCnts = accumulate(sorted.begin(), sorted.end(), 0, [](size_t total, size_t kc) {
        return total + kc;
    });
    const double range = (double)totalCnts / 100;
    size_t percentile = 10;
    size_t partTotal = 0;
    clog << totalCnts << " ops run on " << sorted.size() << " chunks" << endl;
    clog << "max ops per chunk: " << *sorted.begin() <<
            ", min ops per chunk: " << *sorted.rbegin() << ", " <<
            sorted.size() - distance(
                    sorted.begin(),
                    find_if(sorted.begin(), sorted.end(),
                            [](size_t keyCnt) { return keyCnt == 0;})) <<
            " chunk with no ops" << endl;
    for (size_t i = 0; i < sorted.size(); ++i)
    {
        partTotal += sorted[i];
        if (percentile * range <= partTotal)
        {
            clog << setw(3) << percentile << "% of ops obtained by " << setw(8) <<
                    (i <= 1 ? 1 : i - 1) << " chunk (" << fixed << setprecision(1) << 100.0 * i / sorted.size() << "% of chunk)" << endl;
            percentile += 10;
        }
    }
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
