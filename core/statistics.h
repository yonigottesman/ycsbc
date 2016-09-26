/*
 * statistics.h
 *
 *  Created on: Aug 30, 2016
 *      Author: egilad
 */

#ifndef CORE_STATISTICS_H_
#define CORE_STATISTICS_H_

#include <memory>
#include <vector>

namespace ycsbc
{

class Statistics
{
	struct State;
	std::unique_ptr<State> state;
	std::vector<double> partMeans;
	size_t eventCtr;
	
public:
	
	Statistics(size_t expectedEvents);
	Statistics(const Statistics& other);
	~Statistics();
	void addEvent(double time); // NOT thread safe for now (but it's only statistics...)
	void getMeans(double& total, std::vector<double>& partials) const;
};

} /* namespace ycsbc */

#endif /* CORE_STATISTICS_H_ */
