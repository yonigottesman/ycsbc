/*
 * histogram_accumulator.cc
 *
 *  Created on: Apr 2, 2017
 *      Author: erangi
 */

#include "histogram_accumulator.h"
#include <cmath>

using namespace std;

namespace ycsbc
{

HistogramAccumulator::HistogramAccumulator(double minVal_, double maxVal_, size_t bucketsNum) :
        counts(bucketsNum, 0), maxVals(bucketsNum, -1.0), totalOps(0), minVal(minVal_),
		maxVal(maxVal_), bucketRange((maxVal_ - minVal_) / bucketsNum)
{
}

HistogramAccumulator::HistogramAccumulator(const HistogramAccumulator& other) :
        counts(other.counts), maxVals(other.maxVals), totalOps(other.totalOps),
		minVal(other.minVal), maxVal(other.maxVal), bucketRange(other.bucketRange)
{
}

void HistogramAccumulator::addVal(double val)
{
    ++totalOps;
    if (val < minVal)
    {
        ++counts[0]; // place low outliers in the first bucket
        if (val > maxVals[0])
        		maxVals[0]=val;
        return;
    }
    if (val > maxVal)
    {
        ++counts[maxVals.size() - 1]; // place high outliers in the last bucket
        if (val > maxVals[maxVals.size()-1])
        		maxVals[maxVals.size()-1] = val;
        return;
    }
    size_t bucket = (val - minVal) / bucketRange;
    // updates are not thread-safe - keep stats gathering overhead low
    ++counts[bucket];
    if (val > maxVals[bucket])
    		maxVals[bucket] = val;
}

bool HistogramAccumulator::combineHistograms(const HistogramAccumulator& other)
{
    //ensure the histograms have the same range/size:
   if (other.counts.size() != counts.size() ||
           std::abs(minVal - other.minVal) >= 1/(10.0*counts.size()) ||
           std::abs(maxVal - other.maxVal) >= 1/(10.0*counts.size())) {
       return false;
   }

   totalOps += other.totalOps;
   // add other's buckets to this
   for (size_t i = 0; i < counts.size(); i++) {
       counts[i] += other.counts[i];
       if (other.maxVals[i] > maxVals[i])
           maxVals[i] = other.maxVals[i];
   }
   return true;
}

} /* namespace ycsbc */
