//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <core/sys_stats.h>
#include <cstring>
#include <string>
#include <iostream>
#include <sstream>
#include <vector>
#include <future>
#include <algorithm>
#include <iomanip>
#include <exception>
#include <stdexcept>
#include <ctime>
#include <omp.h>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db/db_factory.h"

using namespace std;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
    bool is_loading) {
  db->Init();
  ycsbc::Client client(*db, *wl, num_ops);
  int oks = 0;
  for (int i = 0; i < num_ops; ++i) {
    if (is_loading) {
      oks += client.DoInsert();
    } else {
      oks += client.DoTransaction();
    }
  }
  db->Close();
  return oks;
}

string histToStr(const vector<size_t>& histogram,
        size_t totalVals, double minVal, double bucketSize)
{
    size_t maxHeight = (100 * *max_element(histogram.begin(), histogram.end()))
            / totalVals;
    stringstream ss;
    ss << "Data elements collected: " << totalVals << endl;
    ss << fixed << endl;
    for (size_t b = 0; b < histogram.size(); ++b)
    {
        double n = (100.0 * histogram[b]) / totalVals;
        if (n == 0)
            continue;
        size_t h = n;
        ss << setw(4) << setfill(' ') << setprecision(2)
                << (minVal + bucketSize * b) << ": " << setfill(']') << setw(h)
                << "] " << setfill(' ') << setw(maxHeight - h + 2) << " (" << n
                << "%)" << endl;
    }
    return ss.str();
}

string buildHistogram(const vector<double>& partMeans)
{
    auto minmax = minmax_element(partMeans.begin(), partMeans.end()); //finds smallest and largest keys in partMeans
    double minVal = *minmax.first;
    double maxVal = *minmax.second;
    constexpr size_t BucketsNum = 10;
    double bucketSize = (maxVal - minVal) / BucketsNum; //divides all means among 10 buckets
    vector<size_t> histogram(BucketsNum);
    for (double m : partMeans) //adds means to new histogram
    {
        size_t bucket = (m - minVal) / bucketSize;
        if (bucket == BucketsNum)
            --bucket;
        ++histogram[bucket];
    }
    return histToStr(histogram, partMeans.size(), minVal, bucketSize); //creates string for printing histogram
}

string buildOpsReport(const ycsbc::Client& client)
{
	stringstream ss;
	ss << "Main thread statistics:\n";
	for (int i = 0; i < ycsbc::OPS_NUM; ++i) //for every kind of operation (insert, read, update, scan, rmw)
	{
		double totalMean;
		vector<double> partMeans;
		const ycsbc::Statistics& stats = client.getStats((ycsbc::Operation)i); //get rolling means (uses boost accumulator). Only seems to save 1 out of every 100 results.
		stats.getMeans(totalMean, partMeans);
		if (isnormal(totalMean)) //Determines if the given floating point number arg is normal, i.e. is neither zero, subnormal, infinite, nor NaN.
		{
			string opName = ycsbc::OperationName((ycsbc::Operation)i);
			ss << "Mean " << opName << " time (ms): " << totalMean << endl;
//			ss << "Partial " << opName << " means: ";
//			ss << fixed;
//			for (double t : partMeans)
//				ss << setprecision(2) << t << ", ";
//			ss << "\n";
		}
		if (i == ycsbc::SCAN) //scan's means should take length of scan into account
		{
			double totalScanMean;
			vector<double> partScanMeans;
			const ycsbc::Statistics& scanStats = client.getScanStats(); //get rolling means (uses boost accumulator). Only seems to save 1 out of every 100 results.
			scanStats.getMeans(totalScanMean, partScanMeans);
			if (isnormal(totalScanMean)) //Determines if the given floating point number arg is normal, i.e. is neither zero, subnormal, infinite, nor NaN.
			{
				ss << "Mean scan results: " << totalScanMean << endl;
//				ss << "Partial scan results means: ";
//				for (double t : partScanMeans)
//					ss << t << ", ";
//				ss << "\n";
			}
		}
	    if (!partMeans.empty())
	    {
            ss << "Histogram based on average latency per window: " << endl;
            ss << buildHistogram(partMeans); //builds histogram with 10 buckets, returns printable string representing the histogram
	    }
        const auto& hist = client.getHistogram((ycsbc::Operation)i);
        if (hist.getTotalOps() > 0)
        {
            ss << "Histogram based on predefined time buckets: " << endl; //shows histogram built by client. Currently defined to consider results range 0.0-1.0, with 40 buckets.
            ss << histToStr(hist.getCounts(), hist.getTotalOps(), hist.getMinVal(), hist.getBucketRange());
        }
	}
	return ss.str();
}

string buildIoReport(size_t bytesRead, size_t bytesWritten, const ycsbc::SysStats& beginStats)
{
    stringstream ss;
    ycsbc::SysStats stats = ycsbc::getSysStats();
    stats -= beginStats;
    ss << "key+value bytes sent to DB: " << bytesWritten << ", write amplifications -" <<
            "\n\tvs. bytes sent to kernel:   " << (double)stats.sentWriteBytes / bytesWritten <<
            "\n\tvs. bytes written to disk:  " << (double)stats.diskBytesWritten / bytesWritten <<
            "\nvalue bytes received from DB: " << bytesRead << ", read amplifications -" <<
            "\n\tvs. bytes sent from kernel: " << (double)stats.sentReadBytes / bytesRead <<
            "\n\tvs. bytes read from disk:   " << (double)stats.diskBytesRead / bytesRead <<
            "\ntotal read system calls:  " << stats.callsToRead <<
            "\ntotal write system calls: " << stats.callsToWrite <<
            "\ntotal user time (sec):    " << stats.userTime <<
            "\ntotal system time (sec):  " << stats.sysTime <<
            endl;
    return ss.str();
}

void reportProgress(size_t prog)
{
    time_t t = time(nullptr);
    auto tm = *localtime(&t);
    clog << prog << "% done @ " << put_time(&tm, "%d/%m/%Y %H:%M:%S") << endl;
 //   cout << prog << "% done @ " << put_time(&tm, "%d/%m/%Y %H:%M:%S") << endl;
}

//void periodicProgress(std::atomic<bool>& stopFlag, std::atomic<size_t>& insertCtr, std::atomic<size_t>& transacCtr){
//    while (!stopFlag){
//
//    }
//}

//threadOps - number of operations to be run by each thread.
size_t runOps(const size_t threadsNum, const size_t threadOps,  const bool isLoading,
		ycsbc::DB* db, ycsbc::CoreWorkload& wl, string& report,
		exception_ptr& exceptionThrown)
{
	size_t oks = 0, bytesRead = 0, bytesWritten = 0;
	const size_t reportRange = threadOps / 500; //the thread defined as master (thread 0) will report every 1/100th of the workload it completes.
	ycsbc::SysStats beginStats = ycsbc::getSysStats(); //SysStats count writes to disk, writes in general, user/sys time, calls to read/write
//	ycsbc::Client* clients = new ycsbc::Client*[threadsNum];

	//explanation on pragma omp parallel: https://www.ibm.com/support/knowledgecenter/en/SSGH2K_11.1.0/com.ibm.xlc111.aix.doc/compiler_ref/prag_omp_parallel.html
#pragma omp parallel shared(db, wl, report, exceptionThrown) num_threads(threadsNum) \
	reduction(+: oks) reduction(+: bytesRead) reduction(+: bytesWritten)
	{
		db->Init(); // per-thread initialization (does nothing in either rocksdb or piwi. Empty implementation)
//		clients[omp_get_thread_num] = new ycsbc::Client(*db, wl, threadOps);
		ycsbc::Client client(*db, wl, threadOps);
		const bool isMaster = omp_get_thread_num() == 0;
		for (size_t i = 0; i < threadOps; ++i)
		{
#pragma omp flush (exceptionThrown)
		    if (exceptionThrown != nullptr)
		        break;
		    try
		    {
                if (isLoading) {
                	//calls the CoreGenerator created earlier and generates a key using Generator.next(). Calls the db's insert function to insert.
                	//CoreWorkload::NextSequenceKey(),
                	//counts bytes written
                    oks += client.DoInsert();
                } else {
                	//starts a timer, generates the next op from the core_workload, performs it, ends the timer
                	//counts bytes written, adds timing results to stats and histogram (stats and histogram logging isn't thread safe)
                    oks += client.DoTransaction();
                }
                if ((i + 1) % reportRange == 0 && isMaster)
                    reportProgress((i + 1) *100/ threadOps);
		    }
		    catch (...)
		    {
		        // hopefully, state is consistent enough for all the wrap-up work
		        exceptionThrown = current_exception();
#pragma omp flush (exceptionThrown)
		        break;
		    }
		}
		db->Close(); // per-thread cleanup (empty for piwi and rocksdb)
		if (!isLoading && isMaster)
			report = buildOpsReport(client); //histograms, latency
        bytesRead = client.getBytesRead();
        bytesWritten = client.getBytesWritten();
	}
	report += buildIoReport(bytesRead, bytesWritten, beginStats); //adds more specs to output
	delete[] clients;
	return oks;
}

string durToTime(double usDuration)
{
    size_t msDur = usDuration * 1000;
    size_t millis = msDur % 1000;
    msDur /= 1000;
    tm t = {0};
    t.tm_sec = msDur % 60;
    msDur /= 60;
    t.tm_min = msDur % 60;
    msDur /= 60;
    t.tm_hour = msDur;
    stringstream ss;
    ss << t.tm_hour << ":" << setfill('0') << setw(2) << t.tm_min << ":" <<
            setfill('0') << setw(2) << t.tm_sec << "." << setfill('0') << setw(4) << millis;
    return ss.str();
}

int main(const int argc, const char *argv[]) {
  utils::Properties props;
  string file_name = ParseCommandLine(argc, argv, props);

  //create DB according to the -db flag received
  //initializes db with necessary properties (e.g. for piwi chunk size, munk size,
  //for rocksdb sets rocksdb::options).
  ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }
  else
    cout << "Using DB client of type " << typeid(*db).name() << endl;

  //initialize the Core Workload
  //uses table name, key length, percentage of inserts/gets/updates,
  //number of initial keys, key range, type of key generator (random, zipfian...)
  ycsbc::CoreWorkload wl;
  wl.Init(props);

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));
  const int init_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
  cerr << "Using " << num_threads << " threads" << endl;

  string statsReport;
  exception_ptr exceptionThrown{nullptr};

  //initialization of db - run Ops to intialize.
  //todo nurit - see if can initialize sequentially/randomly and add timing.
  size_t oks = runOps(num_threads, init_ops / num_threads, true, db, wl, statsReport, exceptionThrown);
  cerr << "Loaded " << oks << " records, running workload..." << endl;

  if (exceptionThrown == nullptr)
  {
      // Peforms transactions
      const int total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
      utils::Timer<double> timer;
      timer.Start();
      oks = runOps(num_threads, total_ops / num_threads, false, db, wl, statsReport, exceptionThrown);
      double usDuration = timer.End();
      cerr << "[OVERALL] Run time: " << durToTime(usDuration) << endl;
      cerr << "[OVERALL] Throughput(ops/sec): " << (uint64_t)(total_ops / usDuration) << endl;
      cerr << "[OVERALL] Successful ops: " << oks << " out of " << total_ops << endl;
      cerr << statsReport << endl;
  }
  delete db;
  if (exceptionThrown != nullptr)
      // if execution stopped due to an exception, end the run appropriately.
      // throwing after the reporting to provide more information. this is not safe
      // and probably ruin core dump, but will hopefully be useful on the common case
      rethrow_exception(exceptionThrown);
  return 0;
}

void addProp(int argc, const char* argv[], const char* propName,
		int& argindex, utils::Properties& props)
{
	argindex++;
	if (argindex >= argc)
	{
		UsageMessage(argv[0]);
		exit(1);
	}
	props.SetProperty(propName, argv[argindex]);
	argindex++;
}

bool customProp(int argc, const char* argv[], int& argindex, utils::Properties& props)
{
	if (argv[argindex][0] != '-')
	{
		cout << "Unknown option '" << argv[argindex] << "'" << endl;
		UsageMessage(argv[0]);
		exit(1);
	}
	addProp(argc, argv, &argv[argindex][1], argindex, props);
	return true;
}

string addPropsFromFile(int argc, const char* argv[], int& argindex,
		utils::Properties& props)
{
	argindex++;
	if (argindex >= argc)
	{
		UsageMessage(argv[0]);
		exit(1);
	}
	string filename(argv[argindex]);
	ifstream input(filename);
	try
	{
		props.Load(input);
	}
	catch (const string& message)
	{
		cout << message << endl;
		exit(1);
	}
	argindex++;
	return filename;
}

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
	int argindex = 1;
	string filename;
	stringstream cmdline;
	while (argindex < argc && StrStartWith(argv[argindex], "-"))
	{
	    cmdline << argv[argindex] << " " << argv[argindex + 1] << " ";
		if (strcmp(argv[argindex], "-threads") == 0)
			addProp(argc, argv, "threadcount", argindex, props);
		else if (strcmp(argv[argindex], "-db") == 0)
			addProp(argc, argv, "dbname", argindex, props);
		else if (strcmp(argv[argindex], "-host") == 0)
			addProp(argc, argv, "host", argindex, props);
		else if (strcmp(argv[argindex], "-port") == 0)
			addProp(argc, argv, "port", argindex, props);
		else if (strcmp(argv[argindex], "-slaves") == 0)
			addProp(argc, argv, "slaves", argindex, props);
		else if (strcmp(argv[argindex], "-P") == 0)
			filename = addPropsFromFile(argc, argv, argindex, props);
		else
			customProp(argc, argv, argindex, props);
	}
	cout << "Given command line args: " << cmdline.str() << endl;

	if (argindex == 1 || argindex != argc)
	{
		UsageMessage(argv[0]);
		exit(1);
	}

	return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)" << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple files can" << endl;
  cout << "                   be specified, and will be processed in the order specified" << endl;
  cout << "  -<custom name> val: add a custom property" << endl;
  cout << "Note: the order of the arguments with respect to the properties file matters!" << endl;
  cout << "\nSome custom options:" << endl;
  cout << "  -dbdir: root directory of db files (default: .)" << endl;
  cout << "  -keyrange: range of generated keys (default: depending on distribution)" << endl;
  cout << "RocksDB" << endl;
  cout << "  -rocksdb_usefsync: synchronize file metadata on every sync (default: false)" << endl;
  cout << "  -rocksdb_syncwrites: disable OS buffering and synchronize every file write (default: false)" << endl;
  cout << "  -rocksdb_shards: the number of RocksDB instances to use (default: 1)" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}


