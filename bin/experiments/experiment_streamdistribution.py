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
experimentStartPattern = re.compile("^0");
experimentPattern = re.compile("^(\d+)\t(\d+)")

# Number of nodes 
nodes = 5

class Experiment(object):

	experimntResult = []

	def __init__(self):
		self.experimentResult = []

	def get_std_str(self):
		'''Total elements'''
		totalElements = 0;
		for result in self.experimentResult:
			totalElements = totalElements + result 

		if totalElements == 0:
			return "Error: Elements is 0"

		'''Init Buckets'''
		buckets = [] 
		for no in range(0, nodes):
			buckets.append(0)

		'''Assign data'''
		element = 0
		for result in self.experimentResult:
			buckets[element % nodes] += result;
			element += 1;

		'''Std devision'''
		stdArray = numpy.array(buckets)
		stdDiv = numpy.std(stdArray, ddof=1)
		
		stdPer = float(stdDiv) / float(totalElements) * 100.0

		result = str(len(self.experimentResult)) + "\t" + str(totalElements) + "\t" + str(stdDiv)
		
		for mybucket in range(0, nodes):
			result = result + "\t" + str(buckets[mybucket])

		return result

	def append_experiment_result(self, result):
		self.experimentResult.append(int(result))

# Global variables
experiment = None


''' Handle a line of the input file'''
def handleLine(line):
	global experiment

	experimentStartMatcher = experimentStartPattern.match(line)
	if experimentStartMatcher:
		if not experiment is None:
			print experiment.get_std_str()
		experiment = Experiment()
	
	experimentMatcher = experimentPattern.match(line)
	if experimentMatcher:
		experimentBucket = experimentMatcher.group(2)
		experiment.append_experiment_result(experimentBucket)
print	"#total cells	total buckets	total elements	STD"

''' Read file '''
filename = sys.argv[1]
fh = open(filename, "r")
for line in fh:
	handleLine(line)
fh.close();

''' Print last experiment '''
if not experiment is None:
	print experiment.get_std_str()

