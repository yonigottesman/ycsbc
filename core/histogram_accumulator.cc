/*
 * histogram_accumulator.cc
 *
 *  Created on: Apr 2, 2017
 *      Author: erangi
 */

#include "histogram_accumulator.h"

using namespace std;

namespace ycsbc
{

HistogramAccumulator::HistogramAccumulator(double minVal_, double maxVal_, size_t bucketsNum) :
        counts(bucketsNum, 0), totalOps(0), minVal(minVal_), maxVal(maxVal_),
        bucketRange((maxVal_ - minVal_) / bucketsNum)
{
}

HistogramAccumulator::HistogramAccumulator(const HistogramAccumulator& other) :
        counts(other.counts), totalOps(other.totalOps), minVal(other.minVal),
        maxVal(other.maxVal), bucketRange(other.bucketRange)
{
}

void HistogramAccumulator::addVal(double val)
{
    if (val < minVal)
    {
        ++counts[0]; // place low outliers in the first bucket
        return;
    }
    if (val > maxVal)
    {
        ++counts[counts.size() - 1]; // place high outliers in the last bucket
        return;
    }
    size_t bucket = (val - minVal) / bucketRange;
    // updates are not thread-safe - keep stats gathering overhead low
    ++counts[bucket];
    ++totalOps;
}

} /* namespace ycsbc */
