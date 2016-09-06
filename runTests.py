#!/usr/bin/python

import threading
import subprocess
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

def logOutput(name, out):
    logName = resultsDir + name + '.log'
    f = open(logName, 'w')
    f.write(out.decode('utf-8'))
    f.close()

def getThroughput(name):
    logName = resultsDir + name + '.log'
    tpLineRe = re.compile(r'Throughput')
    tpValRe = re.compile(r'[0-9]+')
    tpPutRe = re.compile(r'Mean Insert time')
    tpGetRe = re.compile(r'Mean Read time')
    tpLatRe = re.compile(r'[0-9]+\.[0-9]+')
    throughput = "0"
    putLat = "0"
    getLat = "0"
    with open(logName) as file:
        for line in file:
            tpline = tpLineRe.search(line)
            if tpline != None:
                throughput = tpValRe.search(line).group()
            else:
                putline = tpPutRe.search(line)
                if putline != None:
                    putLat = tpLatRe.search(line).group()
                else:
                    getline = tpGetRe.search(line)
                    if getline != None:
                        getLat = tpLatRe.search(line).group()
    return (throughput, putLat, getLat)

def recordRun(name, out):
    (throughput, putLat, getLat) = getThroughput(name)
    resultsLock.acquire()
    try:
        f = open(resultsFile, 'a')
        f.write(name.replace('+', '\t') + '\t' + throughput + '\t' + putLat + '\t' + getLat + '\n')
        f.close()
    except Exception as ex:
        print('*** unable to record {0} result: {1} ticks ***'.format(name, ticks))
        print('*** reason: {0} ***'.format(str(ex)))
    resultsLock.release()

def nowStr():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def onExit(out, name):
    print('\t\tRun {0} completed successfully @ {1}'.format(name, nowStr()))
    logOutput(name, out)
    recordRun(name, out)

def onError(ex, name):
    print('-\t-\tRun {0} failed ({1}) @ {2}'.format(name, ex.returncode, nowStr()))
    logOutput(name, ex.output)

def deleteFiles():
    print("deleting data files... ", end="", flush=True)
    [os.remove("./data/" + f) for f in os.listdir("./data")]
    print("done")

def popenAndCall(bench, workload, inits, opers, threads, opt):
    name = runName(bench, workload, inits, opers, threads, opt)
    try:
#               cmd = './ycsbc -db {0} -P ./workloads/{1}.spec -threads {2} -recordcount {3} -operationcount {4} {5}' \
#                       .format(bench, workload, threads, inits, opers, opt)
        cmd = './ycsbc -db {0} -P ../piwi/workloads/{1}.spec -threads {2} -operationcount {3} {4}' \
                .format(bench, workload, threads, opers, opt) # inits omitted! (set in benchmark file)
        print('\t\tStarting: ' + cmd)
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        onExit(out, name)
    except subprocess.CalledProcessError as ex:
        onError(ex, name)
    deleteFiles()
    return


f = open(resultsFile, 'a')
f.write('benchmark\tworkload\tinitNum\topersNum\tthreadsNum\trun_options\tops_per_sec\tputLat\tgetLat\n')
f.close()

#benchmarks = ['piwi', 'rocks']
benchmarks = ['piwi']
#workloads = ['gets_from_keystore', 'gets_from_writebuf', 'puts_and_rebalance', 'puts_gets_rebalances']
workloads = ['puts_gets_rebalances']
initsNum = ['10000']
opersNum =  ['1000000']
threadsNum = ['1', '2', '4', '8', '16']
#options = [''] # must have at least one option, possibly empty
commonOps = '-initChunks 100'
options = [commonOps + ' -workerThreadsNum 0', commonOps + ' -workerThreadsNum 1', commonOps + ' -workerThreadsNum 2', commonOps + ' -workerThreadsNum 4']

# note: on the zipfian distribution, the range of values used to generate keys is initsNum + opersNum * rateOfInsert * 2
# also, default value size is 100 chars (probably configurable via fieldlength, not sure)
for bench in benchmarks:
    for workload in workloads:
        print('Running benchmark {0} with workload {1}, starting at {2}'.format(bench, workload, nowStr()))
        for inits in initsNum:
            for opers in opersNum:
                for threads in threadsNum:
                    opts = (['-initChunks 1'] if bench != 'piwi' else options)
                    for opt in opts:
                        print('\tinits: ' + inits + ', operations: ' + opers + ', threads: ' + threads + ', ops: ' + opt)
                        popenAndCall(bench, workload, inits, opers, threads, opt)

print('All done! @ {0}'.format(nowStr()))
