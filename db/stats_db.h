/*
 * stats_db.h
 *
 *  Created on: Mar 22, 2017
 *      Author: erangi
 */

#ifndef DB_STATS_DB_H_
#define DB_STATS_DB_H_

#include "core/db.h"

#include <map>

namespace ycsbc
{

class StatsDb : public DB
{
    std::map<std::string, size_t> keyCounters;
    const size_t givenMunkKeys = 0;
    const size_t munkBytesCapacity = 0;
    const size_t valSize = 0;

    void printStats(size_t chunkSize);
    void calcMunkKeys(const std::map<std::string, std::string>& props,
            size_t& munkKeys, size_t& munkBytesCapacity, size_t& valSize);

public:
    StatsDb(const std::map<std::string, std::string>& props);
    ~StatsDb();

    int Insert(const std::string &table, const std::string &key,
            std::vector<KVPair> &values) override;

    int Read(const std::string &table, const std::string &key,
            const std::vector<std::string> *fields, std::vector<KVPair> &result)
                    override;

    int Update(const std::string &table, const std::string &key,
            std::vector<KVPair> &values) override;

    int Scan(const std::string &table, const std::string &key, int record_count,
            const std::vector<std::string> *fields,
            std::vector<std::vector<KVPair>> &result) override;

    int Delete(const std::string &table, const std::string &key) override {
        return -1; // should cause an assert to fail
    }
};

} /* namespace ycsbc */

#endif /* DB_STATS_DB_H_ */
