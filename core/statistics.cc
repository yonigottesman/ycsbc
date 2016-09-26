/*
 * statistics.cc
 *
 *  Created on: Aug 30, 2016
 *      Author: egilad
 */

#include "statistics.h"

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/density.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>

#include <iostream>

using namespace boost::accumulators;

namespace ycsbc
{

constexpr size_t WindowsNum = 100;

struct Statistics::State
{
	accumulator_set<double,
		stats<
			tag::mean,
			tag::rolling_mean
		>
	> acc;
	size_t expectingEvents;

	State(size_t expectedEvents) :
		acc(tag::rolling_window::window_size = expectedEvents / WindowsNum),
		expectingEvents(expectedEvents)
	{

	}
};

Statistics::Statistics(size_t expectedEvents) : eventCtr(0)
{
	state.reset(new State(expectedEvents));
	partMeans.reserve(expectedEvents / WindowsNum);
}

Statistics::Statistics(const Statistics& other) : eventCtr(other.eventCtr)
{
	state.reset(new State(other.state->expectingEvents));
	partMeans.reserve(other.partMeans.capacity());
}

Statistics::~Statistics()
{
}

void Statistics::addEvent(double time)
{
	state->acc(time);
	if (++eventCtr % WindowsNum == 0)
	{
		partMeans.push_back(rolling_mean(state->acc));
	}
}

void Statistics::getMeans(double& total, std::vector<double>& partials) const
{
	total = mean(state->acc);
	partials = partMeans;
}

} /* namespace ycsbc */
