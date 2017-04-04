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

class Hash
{
    uint32_t hash = 0;
public:
    Hash(const std::string& str) {
        size_t len = (str.size() < 16) ? str.size() : 16;
        for (size_t i = 0; i < len; ++i)
        {
//            hash = (hash << 1) | (str[i] > '4' ? 1 : 0);
            uint8_t val;
            switch ((int)((str[i] - '0') / 2.5)) {
            case 0:  val = 0; break;
            case 1:  val = 1; break;
            case 2:  val = 2; break;
            default: val = 3; break;
            }
            hash = (hash << 2) | val;
        }
    }
    Hash(const Hash& other) : hash(other.hash) {}
    bool operator<(const Hash& other) const {
        return hash < other.hash;
    }
    bool operator==(const Hash& other) const {
        return hash == other.hash;
    }
    uint32_t hashVal() const {
        return hash;
    }
};

extern size_t usedHash;
extern size_t usedStr;

class HasheKey : public std::string
{
    Hash hash;
public:
    HasheKey(const HasheKey& other) : std::string(other), hash(other.hash) {}
    HasheKey(const std::string& str) : std::string(str), hash(str) {}
    bool operator<(const HasheKey& other) const {
        if (hash == other.hash)
        {
//            ++usedStr;
            return static_cast<const std::string&>(*this) < other;
        }
        else
        {
//            ++usedHash;
            return hash < other.hash;
        }
    }
    const Hash& getHash() const {
        return hash;
    }
};

class StatsDb : public DB
{
    std::map<HasheKey, size_t> keyCounters;
//    std::map<std::string, size_t> keyCounters;
    const size_t givenMunkKeys = 0;
    const size_t munkBytesCapacity = 0;
    const size_t valSize = 0;

    void printStats(size_t chunkSize);
    void calcMunkKeys(const std::map<std::string, std::string>& props,
            size_t& munkKeys, size_t& munkBytesCapacity, size_t& valSize);
    size_t calcChunkAccesses(size_t munkKeys, std::vector<size_t>& sorted);
    std::string manipKey(const std::string& key);

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
