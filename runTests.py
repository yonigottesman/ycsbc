#!/usr/bin/env python3

import threading
import subprocess
from subprocess import Popen
import re
import datetime
import os
try:
  from cStringIO import StringIO
except ImportError:
  from io import StringIO
  
resultsLock = threading.Lock()
resultsDir = os.getenv('HOME') + '/results/'
resultsFile = resultsDir + 'results.txt'

def runName(bench, workload, inits, opers, threads, option):
    if option != '':
            option = '+' + option.replace(' ', '_').replace('-', '')
    return bench + '+' + workload + '+' + inits + '+' + opers + '+' + threads + option

def logName(name):
    logName = resultsDir + name + '.log'
    return logName

def logOutput(name, outStr):
    f = open(logName(name), 'a')
    f.write(outStr)
    f.close()

def getThroughput(name):
    logName = resultsDir + name + '.log'
    tpLineRe = re.compile(r'Throughput')
    tpValRe = re.compile(r'[0-9]+')
    tpInsRe = re.compile(r'Mean Insert time')
    tpUpdRe = re.compile(r'Mean Update time')
    tpGetRe = re.compile(r'Mean Read time')
    tpScanRe= re.compile(r'Mean Scan time')
    tpLatRe = re.compile(r'[0-9]+\.[0-9]+')
    throughput = "0"
    insLat = "0"
    updLat = "0"
    getLat = "0"
    scanLat = "0"
    with open(logName) as file:
        for line in file:
            tpline = tpLineRe.search(line)
            if tpline != None:
                throughput = tpValRe.search(line).group()
            else:
                insline = tpInsRe.search(line)
                if insline != None:
                    insLat = tpLatRe.search(line).group()
                else:
                    updline = tpUpdRe.search(line)
                    if updline != None:
                        updLat = tpLatRe.search(line).group()
                    else:
                        getline = tpGetRe.search(line)
                        if getline != None:
                            getLat = tpLatRe.search(line).group()
                        else:
                            scanline = tpScanRe.search(line)
                            if scanline != None:
                                scanLat = tpLatRe.search(line).group()
    return (throughput, insLat, updLat, getLat, scanLat)

def recordRun(name):
    (throughput, insLat, updLat, getLat, scanLat) = getThroughput(name)
    resultsLock.acquire()
    try:
        f = open(resultsFile, 'a')
        f.write(name.replace('+', '\t') + '\t' + throughput + '\t' + insLat + '\t' + updLat + '\t' + getLat + '\t' + scanLat + '\n')
        f.close()
    except Exception as ex:
        print('*** unable to record {0} result: {1} ticks ***'.format(name, ticks))
        print('*** reason: {0} ***'.format(str(ex)))
    resultsLock.release()

def nowStr():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def onExit(name):
    print('\t\tRun {0} completed successfully @ {1}'.format(name, nowStr()))
    recordRun(name)

def onError(ex, name):
    print('XXX\t\tRun {0} failed ({1}) @ {2}'.format(name, ex.returncode, nowStr()))
    logOutput(name, str(ex))

def deleteFiles():
    print("piwi db size and number of files:\n", end="", flush=True)
    subprocess.call("du -h data_piwi/wb/", shell=True)
    print("\n", end="", flush=True)
    subprocess.call("ls -l data_piwi/wb/ | wc -l", shell=True)
    print("rocks db size and number of files:\n", end="", flush=True)
    subprocess.call("du -h data_rocksdb/", shell=True)
    print("\n", end="", flush=True)
    subprocess.call("ls -l data_rocksdb/ | wc -l", shell=True)
    print("deleting data files... ", end="", flush=True)
    subprocess.call("rm -f data_rocksdb/*", shell=True)
    subprocess.call("find data_piwi/wb -name '~*' -delete", shell=True)
    print("done")

def popenAndCall(bench, workload, inits, opers, threads, opt):
    name = runName(bench, workload, inits, opers, threads, opt)
    try:
        cmd = './ycsbc -db {0} -P ../piwi/workloads/{1}.spec -threads {2} -recordcount {3} -operationcount {4} {5}' \
			   .format(bench, workload, threads, inits, opers, opt)
        print('\t\tStarting: ' + cmd)
        with Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1, shell=True) as p, \
             open(logName(name), 'ab', buffering=0) as file:
            for line in p.stdout: # b'\n'-separated lines
#                sys.stdout.buffer.write(line) # pass bytes as is
                file.write(line)
            if p.wait() != 0:
                raise subprocess.CalledProcessError(p.returncode, cmd)
        onExit(name)
    except subprocess.CalledProcessError as ex:
        onError(ex, name)
    deleteFiles()
    return


f = open(resultsFile, 'a')
f.write('benchmark\tworkload\tinitNum\topersNum\tthreadsNum\trun_options\tops_per_sec\tinsLat\tupdLat\tgetLat\tscanLat\n')
f.close()

benchmarks = ['piwi', 'rocks']
threadsNum = ['16']
commonOps = ''
piwi_commonOps = ''
piwi_options = ['-munkBytesCapacity 196608 -maxMunks 1800 -writeBufBytesCapacity 524288']
rocks_options = ['-rocksdb_cachesize 8140000000']

############### inserts only #############
workloads = ['seq_inserts']
initsNum = ['10']
opersNum = ['100000000']

for bench in benchmarks:
    for workload in workloads:
        print('Running benchmark {0} with workload {1}, starting at {2}'.format(bench, workload, nowStr()))
        for inits in initsNum:
            for opers in opersNum:
                for threads in threadsNum:
                    opts = (piwi_options if bench == 'piwi' else rocks_options)
                    for opt in opts:
                        opt = opt + ' ' + commonOps
                        print('\tinits: ' + inits + ', operations: ' + opers + ', threads: ' + threads + ', ops: ' + opt)
                        popenAndCall(bench, workload, inits, opers, threads, opt)

############### puts only #############
workloads = ['puts_only']
initsNum = ['100000000']
opersNum = ['100000000']

for bench in benchmarks:
    for workload in workloads:
        print('Running benchmark {0} with workload {1}, starting at {2}'.format(bench, workload, nowStr()))
        for inits in initsNum:
            for opers in opersNum:
                for threads in threadsNum:
                    opts = (piwi_options if bench == 'piwi' else rocks_options)
                    for opt in opts:
                        opt = opt + ' ' + commonOps
                        print('\tinits: ' + inits + ', operations: ' + opers + ', threads: ' + threads + ', ops: ' + opt)
                        popenAndCall(bench, workload, inits, opers, threads, opt)

############### read only #############
workloads = ['rand_reads']
initsNum = ['100000000']
opersNum = ['300000000']

for bench in benchmarks:
    for workload in workloads:
        print('Running benchmark {0} with workload {1}, starting at {2}'.format(bench, workload, nowStr()))
        for inits in initsNum:
            for opers in opersNum:
                for threads in threadsNum:
                    opts = (piwi_options if bench == 'piwi' else rocks_options)
                    for opt in opts:
                        opt = opt + ' ' + commonOps
                        print('\tinits: ' + inits + ', operations: ' + opers + ', threads: ' + threads + ', ops: ' + opt)
                        popenAndCall(bench, workload, inits, opers, threads, opt)

############### scans only #############
workloads = ['scans_only']
initsNum = ['100000000']
opersNum = ['100000000']

for bench in benchmarks:
    for workload in workloads:
        print('Running benchmark {0} with workload {1}, starting at {2}'.format(bench, workload, nowStr()))
        for inits in initsNum:
            for opers in opersNum:
                for threads in threadsNum:
                    opts = (piwi_options if bench == 'piwi' else rocks_options)
                    for opt in opts:
                        opt = opt + ' ' + commonOps
                        print('\tinits: ' + inits + ', operations: ' + opers + ', threads: ' + threads + ', ops: ' + opt)
                        popenAndCall(bench, workload, inits, opers, threads, opt)


############### mixes #############
workloads = ['puts_gets', 'puts_gets_scans']
initsNum = ['100000000']
opersNum = ['100000000']

for bench in benchmarks:
    for workload in workloads:
        print('Running benchmark {0} with workload {1}, starting at {2}'.format(bench, workload, nowStr()))
        for inits in initsNum:
            for opers in opersNum:
                for threads in threadsNum:
                    opts = (piwi_options if bench == 'piwi' else rocks_options)
                    for opt in opts:
                        opt = opt + ' ' + commonOps
                        print('\tinits: ' + inits + ', operations: ' + opers + ', threads: ' + threads + ', ops: ' + opt)
                        popenAndCall(bench, workload, inits, opers, threads, opt)

print('All done! @ {0}'.format(nowStr()))
