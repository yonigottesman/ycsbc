/*
 * histogram_accumulator.h
 *
 *  Created on: Apr 2, 2017
 *      Author: erangi
 */

#ifndef CORE_HISTOGRAM_ACCUMULATOR_H_
#define CORE_HISTOGRAM_ACCUMULATOR_H_

#include <vector>
#include <cstddef>

namespace ycsbc
{

class HistogramAccumulator
{
    std::vector<size_t> counts;
    size_t totalOps;
    const double minVal;
    const double maxVal;
    const double bucketRange;
public:
    HistogramAccumulator(double minVal, double maxVal, size_t bucketsNum);
    HistogramAccumulator(const HistogramAccumulator& other);
    void addVal(double val);
    const std::vector<size_t>& getCounts() const {
        return counts;
    }
    size_t getTotalOps() const {
        return totalOps;
    }
    double getBucketRange() const {
        return bucketRange;
    }
    double getMinVal() const {
        return minVal;
    }
};

} /* namespace ycsbc */

#endif /* CORE_HISTOGRAM_ACCUMULATOR_H_ */
