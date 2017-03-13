/*
 * io_stats.h
 *
 *  Created on: Mar 6, 2017
 *      Author: erangi
 */

#ifndef CORE_SYS_STATS_H_
#define CORE_SYS_STATS_H_

#include <cstddef>

namespace ycsbc
{

struct SysStats
{
    // sent* are byte moved between the program and the system. those moves could
    // originate or end at the page cache etc., i.e., not necessarily the disk itself
    size_t sentReadBytes = 0;
    size_t sentWriteBytes = 0;
    // disk* are bytes actually read from the disk or written to it. the counts might
    // include operations not initiated by the program, e.g., read aheads
    size_t diskBytesRead = 0;
    size_t diskBytesWritten = 0;
    size_t callsToRead = 0;
    size_t callsToWrite = 0;

    size_t userTime = 0;
    size_t sysTime = 0;

    SysStats& operator-=(const SysStats& other) {
        sentReadBytes -= other.sentReadBytes;
        sentWriteBytes -= other.sentWriteBytes;
        diskBytesRead -= other.diskBytesRead;
        diskBytesWritten -= other.diskBytesWritten;
        callsToRead -= other.callsToRead;
        callsToWrite -= other.callsToWrite;
        userTime -= other.userTime;
        sysTime -= other.sysTime;
        return *this;
    }
};

SysStats getSysStats();

} /* namespace ycsbc */

#endif /* CORE_SYS_STATS_H_ */
