#!/usr/bin/python

import threading
import subprocess
import re
import datetime
import os
try:
  import StringIO
except ImportError:
  from io import StringIO
  
resultsLock = threading.Lock()
resultsDir = os.getenv('HOME') + '/results/'
resultsFile = resultsDir + 'results.txt'

def runName(bench, workload, inits, opers, threads, option):
	if option != '':
		option = '_' + option.replace(' ', '_').replace('-', '')
	return bench + '_' + workload + '_' + inits + '_' + opers + '_' + threads + option

def logOutput(name, out):
	logName = resultsDir + name + '.log'
	f = open(logName, 'w')
	f.write(out.decode('utf-8'))
	f.close()

def getThroughput(out):
	lre = re.compile(r'Throughput')
	tre = re.compile(r'[0-9]+')
	s = StringIO.StringIO(out)
	for line in s:
		tpline = lre.search(line)
		if tpline != None:
			print('throughput line: {0}'.format(tpline))
			throughput = tre.search(line).group()
			return throughput

def recordRun(name, out):
	ticks = getThroughput(out)
	resultsLock.acquire()
	try:
		f = open(resultsFile, 'a')
		f.write(name.replace('_', '\t') + '\t' + ticks + '\n')
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

def popenAndCall(bench, workload, inits, opers, threads, opt):
	name = runName(bench, workload, inits, opers, threads, opt)
	try:
		cmd = './ycsbc -db {0} -P ./workloads/{1}.spec -threads {2} -recordcount {3} -operationcount {4} {5}' \
  			.format(bench, workload, threads, inits, opers, opt)
		print('\t\tStarting: ' + cmd)
		out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
		onExit(out, name)
	except subprocess.CalledProcessError as ex:
		onError(ex, name)
	return


f = open(resultsFile, 'a')
f.write('benchmark\tworkload\tinitNum\topersNum\tthreadsNum\toptions\tticks\n')
f.close()

benchmarks = ['piwi', 'rocks']
workloads = ['workloadp']
initsNum = ['2000000']
opersNum =  ['10000000']
threadsNum = ['1', '2', '4', '8', '16']
#options = [''] # must have at least one option, possibly empty
options = ['-initChunks 1', '-initChunks 10', '-initChunks 100']

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
