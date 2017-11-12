#!/usr/bin/env python3

import threading
import subprocess
from subprocess import Popen
import re
import datetime
import os
import shutil
try:
  from cStringIO import StringIO
except ImportError:
  from io import StringIO
  
resultsLock = threading.Lock()
piwiPath = '/home-local/erangi/yahoo/YCSB-C/'
piwiDataDir = piwiPath+'data_piwi'
resultsDir = os.getenv('HOME') + '/YCSB-C/results' + format(datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')) + '/'
resultsFile = resultsDir + 'results.txt'

def runName(bench, workload, inits, opers, dist, threads, option):
    if option != '':
            option = '+' + option.replace(' ', '_').replace('-', '')
    return bench + '+' + workload + '+' + inits + '+' + opers + '+' + dist + '+' + threads + option

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
    tpPerc10 = re.compile(r'10 percentile in') 
    tpPerc20 = re.compile(r'20 percentile in') 
    tpPerc30 = re.compile(r'30 percentile in') 
    tpPerc40 = re.compile(r'40 percentile in') 
    tpPerc50 = re.compile(r'50 percentile in') 
    tpPerc60 = re.compile(r'60 percentile in') 
    tpPerc70 = re.compile(r'70 percentile in') 
    tpPerc80 = re.compile(r'80 percentile in') 
    tpPerc90 = re.compile(r'90 percentile in') 
    tpPerc95 = re.compile(r'95 percentile in')
    tpPerc98 = re.compile(r'98 percentile in')
    tpPerc99 = re.compile(r'99 percentile in')
    tpPercRe = re.compile(r'\[.*\]')
    throughput = "0"
    insLat = "0"
    updLat = "0"
    getLat = "0"
    scanLat = "0"
    perc10 = ' '
    perc20 = ' '
    perc30 = ' '
    perc40 = ' '
    perc50 = ' '
    perc60 = ' '
    perc70 = ' '
    perc80 = ' '
    perc90 = ' '
    perc95 = ' '
    perc98 = ' '
    perc99 = ' '
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
                            else:
                                scanline = tpPerc10.search(line)
                                if scanline != None:
                                    perc10 = tpPercRe.search(line).group()
                                else:
                                    scanline = tpPerc20.search(line)
                                    if scanline != None:
                                        perc20 = tpPercRe.search(line).group()
                                    else:
                                        scanline = tpPerc30.search(line)
                                        if scanline != None:
                                            perc30 = tpPercRe.search(line).group()
                                        else:
                                            scanline = tpPerc40.search(line)
                                            if scanline != None:
                                                perc40 = tpPercRe.search(line).group()
                                            else:
                                                scanline = tpPerc50.search(line)
                                                if scanline != None:
                                                    perc50 = tpPercRe.search(line).group()
                                                else:
                                                    scanline = tpPerc60.search(line)
                                                    if scanline != None:
                                                        perc60 = tpPercRe.search(line).group()
                                                    else:
                                                        scanline = tpPerc70.search(line)
                                                        if scanline != None:
                                                            perc70 = tpPercRe.search(line).group()
                                                        else:
                                                            scanline = tpPerc80.search(line)
                                                            if scanline != None:
                                                                perc80 = tpPercRe.search(line).group()
                                                            else:
                                                                scanline = tpPerc90.search(line)
                                                                if scanline != None:
                                                                    perc90 = tpPercRe.search(line).group()
                                                                else:
                                                                    scanline = tpPerc95.search(line)
                                                                    if scanline != None:
                                                                        perc95 = tpPercRe.search(line).group()
                                                                    else:
                                                                        scanline = tpPerc98.search(line)
                                                                        if scanline != None:
                                                                            perc98 = tpPercRe.search(line).group()
                                                                        else:
                                                                            scanline = tpPerc99.search(line)
                                                                            if scanline != None:
                                                                                perc99 = tpPercRe.search(line).group()
 
                          
    return (throughput, insLat, updLat, getLat, scanLat, perc10, perc20, perc30, perc40, perc50, perc60, perc70, perc80, perc90, perc95, perc98, perc99)

def recordRun(name):
    (throughput, insLat, updLat, getLat, scanLat, perc10, perc20, perc30, perc40, perc50, perc60, perc70, perc80, perc90, perc95, perc98, perc99) = getThroughput(name)
    resultsLock.acquire()
    try:
        f = open(resultsFile, 'a')
        f.write(name.replace('+', '\t') + '\t' + throughput + '\t' + insLat + '\t' + updLat + '\t' + getLat + '\t' + scanLat + '\t' + perc10 + '\t' + perc20 + '\t' + perc30 + '\t' + perc40 + '\t' + perc50 + '\t' + perc60 + '\t' + perc70 + '\t' + perc80 + '\t' + perc90 + '\t' + perc95 + '\t' +  perc98 + '\t' + perc99 + '\n')
        f.close()
        
        print('name is ' + name + ' name.find is ')
        if name.find('rocks') != -1:
            shutil.copyfile('data_rocksdb/LOG', resultsDir+name + '+LOG')
            
    except Exception as ex:
        print('*** unable to record {0}  ***'.format(name))
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
    subprocess.call("du -h " + piwiDataDir+ "/wb/", shell=True)
    print("\n", end="", flush=True)
    subprocess.call("ls -l " + piwiDataDir+ "/wb/ | wc -l", shell=True)
    print("\n", end="", flush=True)
    print("deleting data files... ", end="", flush=True)
    subprocess.call("find " + piwiDataDir+ "/ -name '*store*' -delete", shell=True)    
    subprocess.call("find " + piwiDataDir+ "/ -name '*REST*' -delete", shell=True)    
    print("done")

def popenAndCall(bench, workload, inits, opers, msize, threads, opt, split, serial):
    name = runName(bench, workload, inits, opers, msize, threads, opt)
    try:
        dbPath = "piwiSavedDbs/" +split+"-"+ msize+ "-" + serial
        print('About to copy DB from '+dbPath + ' to ' + piwiDataDir)
        
        if not os.path.exists(piwiDataDir):
             os.makedirs(piwiDataDir)#subprocess.call("rsync -a " + dbPath + "/ " + piwiDataDir + "/", shell=True)
        subprocess.call("tar -C "+piwiDataDir+" -xzf "+dbPath+"/savedDb.tar.gz", shell=True)
        print('Finished copying db. Executing:')
 
        cmd = './ycsbc -db {0} -P workloads/{1}.spec -threads {2} -recordcount {3} -operationcount {4} -munkBytesCapacity {5} -dbdir {7} {6}' \
             .format(bench, workload, threads, inits, opers, msize, opt, piwiPath)
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


if not os.path.exists(resultsDir):
    os.makedirs(resultsDir)

f = open(resultsFile, 'a')
f.write('benchmark\tworkload\tinitNum\topersNum\tthreadsNum\trun_options\tops_per_sec\tinsLat\tupdLat\tgetLat\tscanLat\tpercentile10\tpercentile20\tpercentile30\tpercentile40\tpercentile50\tpercentile60\tpercentile70\tpercentile80\tpercentile90\tpercentile95\tpercentile98\tpercentile99\n')
f.close()








benchmarks = ['piwi']
threadsNum = [ '28']
commonOps = ''
piwi_commonOps = ''
keyrange = '15000000'


############# WITH PRESPLIT: ############################
################ random initialization of dbse  #############
#opersNum = ['600000000'] 
#initsNum = ['0'] 
#workloads = ['gets_only_zip', 'puts_gets_zip', 'puts_only_zip']
#piwi_options = ['-maxMunks 4000 -writeBufBytesCapacity 524288 -initChunks 50000 -use_existing_db true'] 
#munk_sizes = ['20166'] #500KB 
 
#for workload in workloads:
#    for bench in benchmarks:
#        print('Running benchmark {0} with workload {1}, starting at {2}'.format(bench, workload, nowStr()))
#        for inits in initsNum:
#            for opers in opersNum:
#                for msize in munk_sizes:
#                    for threads in threadsNum:
#                        opts = (piwi_options if bench == 'piwi' else rocks_options)
#                        for opt in opts:
#                            opt = opt + ' ' + commonOps
#                            print('\tinits: ' + inits + ', operations: ' + opers + ', munk_bytes_capacity: ' + msize + ', threads: ' + threads + ', ops: ' + opt)
#                            popenAndCall(bench, workload, inits, opers, msize, threads, opt, "Presplit", "random")

#########################################

opersNum = ['60000000'] 
initsNum = ['0'] 
workloads = ['scans_only_256_uniform', 'scans_only_8192_uniform', 'scans_only_16384_uniform']#, 'puts_scans_256_uniform', 'puts_scans_8192_uniform', 'puts_scans_16384_uniform']
piwi_options = ['-maxMunks 4000 -writeBufBytesCapacity 524288 -initChunks 50000 -use_existing_db true'] 
munk_sizes = ['20166'] #500KB 

for workload in workloads:
    for bench in benchmarks:
        print('Running benchmark {0} with workload {1}, starting at {2}'.format(bench, workload, nowStr()))
        for inits in initsNum:
            for opers in opersNum:
                for msize in munk_sizes:
                    for threads in threadsNum:
                        opts = (piwi_options if bench == 'piwi' else rocks_options)
                        for opt in opts:
                            opt = opt + ' ' + commonOps
                            print('\tinits: ' + inits + ', operations: ' + opers + ', munk_bytes_capacity: ' + msize + ', threads: ' + threads + ', ops: ' + opt)
                            popenAndCall(bench, workload, inits, opers, msize, threads, opt, "Presplit", "random")



############# WITHOUT PRESPLIT: ############################
################ random initialization of dbse  #############
opersNum = ['600000000'] 
initsNum = ['0'] 
workloads = ['gets_only_zip', 'puts_gets_zip', 'puts_only_zip']
piwi_options = ['-maxMunks 4000 -writeBufBytesCapacity 524288 -initChunks 5 -use_existing_db true'] 
munk_sizes = ['20166'] #500KB 

for workload in workloads:
    for bench in benchmarks:
        print('Running benchmark {0} with workload {1}, starting at {2}'.format(bench, workload, nowStr()))
        for inits in initsNum:
            for opers in opersNum:
                for msize in munk_sizes:
                    for threads in threadsNum:
                        opts = (piwi_options if bench == 'piwi' else rocks_options)
                        for opt in opts:
                            opt = opt + ' ' + commonOps
                            print('\tinits: ' + inits + ', operations: ' + opers + ', munk_bytes_capacity: ' + msize + ', threads: ' + threads + ', ops: ' + opt)
                            popenAndCall(bench, workload, inits, opers, msize, threads, opt, "NoPresplit", "random")


opersNum = ['60000000']
initsNum = ['0']
workloads = ['scans_only_256_uniform', 'scans_only_8192_uniform', 'scans_only_16384_uniform', 'puts_scans_256_uniform', 'puts_scans_8192_uniform', 'puts_scans_16384_uniform']
piwi_options = ['-maxMunks 4000 -writeBufBytesCapacity 524288 -initChunks 5 -use_existing_db true']
munk_sizes = ['20166'] #500KB 

for workload in workloads:
    for bench in benchmarks:
        print('Running benchmark {0} with workload {1}, starting at {2}'.format(bench, workload, nowStr()))
        for inits in initsNum:
            for opers in opersNum:
                for msize in munk_sizes:
                    for threads in threadsNum:
                        opts = (piwi_options if bench == 'piwi' else rocks_options)
                        for opt in opts:
                            opt = opt + ' ' + commonOps
                            print('\tinits: ' + inits + ', operations: ' + opers + ', munk_bytes_capacity: ' + msize + ', threads: ' + threads + ', ops: ' + opt)
                            popenAndCall(bench, workload, inits, opers, msize, threads, opt, "NoPresplit", "random")

print('All done! @ {0}'.format(nowStr()))
