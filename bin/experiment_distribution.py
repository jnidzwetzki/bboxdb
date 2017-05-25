#!/usr/bin/python
#
# Calculate the statistical values for the 
# distribution experiment
#
############################################

import sys
import re
import math
import numpy

# Check args
if len(sys.argv) < 2:
	print "Usage:", sys.argv[0], "<filename>"
        sys.exit(0)

# Regex
samplingSizePattern = re.compile("^# Simulating with max element size: ([\d\.]+)");
experimentPattern = re.compile("^(\d+)\s(\d+)")


class Experiment(object):

	samplingSize = -1
	experimntResult = []

	def __init__(self, samplingSize):
		self.samplingSize = samplingSize
		self.experimentResult = []

	def get_std_str(self):
		'''Total elements'''
		totalElements = 0;
		for result in self.experimentResult:
			totalElements = totalElements + result 

		'''Std devision'''
		stdArray = numpy.array(self.experimentResult)
		stdDiv = numpy.std(stdArray, ddof=1)
		
		stdPer = float(stdDiv) / float(totalElements) * 100.0

		return self.samplingSize + "\t" + str(len(self.experimentResult)) + "\t" + str(totalElements) + "\t" + str(stdDiv)

	def __str__(self):
		return self.get_std_str()

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
		experimentBucketSize = experimentMatcher.group(2)
		experiment.append_experiment_result(experimentBucketSize)
#		print line,

	sampleMatcher = samplingSizePattern.match(line)
	if sampleMatcher:
		if not experiment is None:
			print experiment
		experiment = Experiment(sampleMatcher.group(1))

print	"#Max elements	total buckets	total elements	STD"

''' Read file '''
filename = sys.argv[1]
fh = open(filename, "r")
for line in fh:
	handleLine(line)
fh.close();

''' Print last experiment '''
if not experiment is None:
	print experiment

