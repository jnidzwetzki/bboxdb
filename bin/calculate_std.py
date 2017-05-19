#!/usr/bin/python
#
# Calculate the standard deviation of the 
# sampling size experiment
#
############################################

import sys
import re
import numpy

# Check args
if len(sys.argv) < 2:
	print "Usage " + sys.argv[0] + " <filename>"
        sys.exit(0)

# Regex
samplingSizePattern = re.compile("^Simulating with sample size: ([\d\.]+)");
experimentPattern = re.compile("^(\d+)\s(\d+)\s\d+\s\d+\s(\d+)")


class Experiment(object):

	samplingSize = -1
	diffData = []
	totalElements = -1

	def __init__(self, samplingSize):
		self.samplingSize = samplingSize

	def get_std_str(self):
		'''Error in STD'''
		npa = numpy.array(self.diffData)
		std = numpy.std(npa)
		stdPer = std / self.totalElements * 100.0

		'''Error on AVG'''
		totalDiff = 0;
		for result in self.diffData:
			totalDiff = totalDiff + result

		average = totalDiff / len(self.diffData)
		averagePer = float(average) / float(self.totalElements) * 100.0

		return  self.samplingSize + "\t" + str(stdPer) + "\t" + str(averagePer)

	def __str__(self):
		return self.get_std_str()

	def set_total_elements(self, totalElements):
		self.totalElements = int(totalElements)

	def append_experiment_result(self, result):
		self.diffData.append(int(result))

# Global variables
experiment = None


''' Handle a line of the input file'''
def handleLine(line):
	global experiment

	experimentMatcher = experimentPattern.match(line)
	if experimentMatcher:
		experimentRun = experimentMatcher.group(1)
		experimentTotal = experimentMatcher.group(2)
		experimentDiff = experimentMatcher.group(3)
		experiment.set_total_elements(experimentTotal)
		experiment.append_experiment_result(experimentDiff)
#		print line,

	sampleMatcher = samplingSizePattern.match(line)
	if sampleMatcher:
		if not experiment is None:
			print experiment
		experiment = Experiment(sampleMatcher.group(1))

print	"#Sampling size		STD error	AVG errror"

''' Read file '''
filename = sys.argv[1]
fh = open(filename, "r")
for line in fh:
	handleLine(line)

fh.close();

''' Print last experiment '''
if not experiment is None:
	print experiment

