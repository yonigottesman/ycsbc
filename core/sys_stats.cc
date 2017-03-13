/*
 * io_stats.cpp
 *
 *  Created on: Mar 6, 2017
 *      Author: erangi
 */

#include "sys_stats.h"

#include <sys/types.h>
#include <sys/resource.h>
#include <unistd.h>
#include <string>
#include <fstream>
#include <sstream>

using namespace std;

namespace ycsbc
{

namespace
{

bool readIoStat(istream& procIo, const char* statName, size_t& statVal)
{
    string category, value;
    getline(procIo, category, ':');
    if (category != statName)
        return false;
    getline(procIo, value);
    stringstream ss(value);
    ss >> statVal;
    return !!ss;
}

void populateIoStats(pid_t pid, SysStats& stats)
{
    string fileName("/proc/" + to_string(pid) + "/io");
    ifstream procIo(fileName);
    if (!procIo)
        return;
    //  see details at http://git.kernel.org/cgit/linux/kernel/git/torvalds/linux.git/tree/Documentation/filesystems/proc.txt
    readIoStat(procIo, "rchar", stats.sentReadBytes);
    readIoStat(procIo, "wchar", stats.sentWriteBytes);
    readIoStat(procIo, "syscr", stats.callsToRead);
    readIoStat(procIo, "syscw", stats.callsToWrite);
    readIoStat(procIo, "read_bytes", stats.diskBytesRead);
    readIoStat(procIo, "write_bytes", stats.diskBytesWritten);
}

void populateTimeStats(pid_t pid, SysStats& stats)
{
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    stats.userTime = usage.ru_utime.tv_sec;
    stats.sysTime = usage.ru_stime.tv_sec;
}

} // anonymous namespace

SysStats getSysStats()
{
    SysStats stats;

    pid_t pid = getpid();
    populateIoStats(pid, stats);
    populateTimeStats(pid, stats);
    return stats;
}

} /* namespace ycsbc */
