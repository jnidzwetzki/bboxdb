#!/usr/bin/python
#
# Calculate the statistical values for the 
# fixed cell experiment
#
############################################

import sys
import re
import math
import numpy
from array import array

# Check args
if len(sys.argv) < 2:
	print "Usage:", sys.argv[0], "<filename>"
        sys.exit(0)

# Regex
experimentStartPattern = re.compile("#Cell");
experimentPattern = re.compile("^(\d+)")

# Number of buckets 
allBuckets = array("i", [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8096])

class Experiment(object):

	cells = -1
	experimntResult = []

	def __init__(self):
		self.experimentResult = []

	def get_std_str(self, noOfBuckets):
		'''Total elements'''
		totalElements = 0;
		for result in self.experimentResult:
			totalElements = totalElements + result 

		if totalElements == 0:
			return "Error: Elements is 0"

		'''Init Buckets'''
		buckets = [] 
		for no in range(0, noOfBuckets):
			buckets.append(0)

		'''Assign data'''
		element = 0
		for result in self.experimentResult:
			buckets[element % noOfBuckets] += result;
			element += 1;

		'''Std devision'''
		stdArray = numpy.array(buckets)
		stdDiv = numpy.std(stdArray, ddof=1)
		
		stdPer = float(stdDiv) / float(totalElements) * 100.0

		return str(len(self.experimentResult)) + "\t" + str(noOfBuckets) + "\t" + str(totalElements) + "\t" + str(stdDiv)

	def append_experiment_result(self, result):
		self.experimentResult.append(int(result))

# Global variables
experiment = None


''' Handle a line of the input file'''
def handleLine(line):
	global experiment

	experimentMatcher = experimentPattern.match(line)
	if experimentMatcher:
		experimentBucket = experimentMatcher.group(1)
		experiment.append_experiment_result(experimentBucket)

	experimentStartMatcher = experimentStartPattern.match(line)
	if experimentStartMatcher:
		if not experiment is None:
			for bucket in allBuckets:
				print experiment.get_std_str(bucket)
		experiment = Experiment()

print	"#total cells	total buckets	total elements	STD"

''' Read file '''
filename = sys.argv[1]
fh = open(filename, "r")
for line in fh:
	handleLine(line)
fh.close();

''' Print last experiment '''
if not experiment is None:
	for bucket in allBuckets:
		print experiment.get_std_str(bucket)

