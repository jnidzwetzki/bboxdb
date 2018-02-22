/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *    
 *******************************************************************************/
package org.bboxdb.experiments;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.FileLineIndex;
import org.bboxdb.tools.TupleFileReader;
import org.bboxdb.tools.converter.tuple.TupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DetermineSamplingSize implements Runnable {

	/**
	 * The file to import
	 */
	protected final String filename;
	
	/**
	 * The format of the input file
	 */
	protected String format;
	
	/**
	 * The retry counter for the experiments
	 */
	public final static int EXPERIMENT_RETRY = 50;
	
	/**
	 * The maximal number of elements to process
	 */
	public final static long MAX_ELEMENTS = 2000000;
	
	/**
	 * The file line index
	 */
	private FileLineIndex fli = null;
	
	/**
	 * The dimension of the tuples
	 */
	private int tupleDimension = -1;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DetermineSamplingSize.class);
	
	public DetermineSamplingSize(final String filename, final String format) throws IOException {
		this.filename = filename;
		this.format = format;
	}
	
	@Override
	public void run() {
		try {
			System.out.format("Indexing %s\n", filename);
			fli = new FileLineIndex(filename);
			fli.indexFile();
			System.out.format("Indexing %s done\n", filename);

			final List<Double> sampleSizes = Arrays.asList(
					0.01d, 0.02d, 0.03d, 0.04d, 0.05d,
					0.06d, 0.07d, 0.08d, 0.09d,
					0.1d, 0.2d, 0.3d, 0.4d, 0.5d, 
					0.6d, 0.7d, 0.8d, 0.9d, 1.0d, 
					2.0d, 3.0d, 4.0d, 5.0d, 6.0d, 
					7.0d, 8.0d, 9.0d, 10.0d);
					//5d, 10d, 20d, 30d, 40d, 50d, 60d);
			
			sampleSizes.forEach(s -> runExperiment(s));
		} catch (IOException e) {
			logger.error("Got an IO Exception", e);
			System.exit(-1);
		}
	}

	/**
	 * Run the experiment with the given sample size
	 * @param sampleSize
	 */
	protected void runExperiment(final double sampleSize) {
		System.out.println("Simulating with sample size: " + sampleSize);

		try {
			final long linesInFile = fli.getIndexedLines();
			final long numberOfElements = Math.min(linesInFile, MAX_ELEMENTS);
			final int numberOfSamples = (int) (numberOfElements / 100 * sampleSize);
			
			final ExperimentSeriesStatistics experimentSeriesStatistics = new ExperimentSeriesStatistics();
			
			ExperimentStatistics.printExperientHeader();
			for(int experiment = 0; experiment < EXPERIMENT_RETRY; experiment++) {
				final double splitPos = getSplit(numberOfSamples, numberOfElements);
				final ExperimentStatistics statistics = runExperimentForPos(splitPos);
				statistics.printExperimentResult(experiment);
				experimentSeriesStatistics.addExperiment(statistics);		
			}
			
			experimentSeriesStatistics.printStatistics();
			System.out.println("\n\n");
			
		} catch (ClassNotFoundException | IOException e) {
			System.err.println(e.getStackTrace());
		}
	}

	/**
	 * Run the experiment for the given position
	 * @param splitPos
	 * @return 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	protected ExperimentStatistics runExperimentForPos(final double splitPos) throws ClassNotFoundException, IOException {

		final ExperimentStatistics statistics = new ExperimentStatistics();		
		final BoundingBox fullBox = BoundingBox.createFullCoveringDimensionBoundingBox(tupleDimension);
		final BoundingBox leftBox = fullBox.splitAndGetLeft(splitPos, 0, true);
		final BoundingBox rightBox = fullBox.splitAndGetRight(splitPos, 0, false);

		final TupleFileReader tupleFile = new TupleFileReader(filename, format);
		
		tupleFile.addTupleListener(t -> {
			final BoundingBox polygonBoundingBox = t.getBoundingBox();
			
			boolean tupleDistributed = false;
			
			if(polygonBoundingBox.overlaps(leftBox)) {
				statistics.increaseLeft();
				tupleDistributed = true;
			}
			
			if(polygonBoundingBox.overlaps(rightBox)) {
				statistics.increaseRight();
				tupleDistributed = true;
			}
			
			statistics.increaseTotal();
			
			if(!tupleDistributed) {
				System.err.println("Unable to distribute: ");
				System.err.println("Left box: " + leftBox);
				System.err.println("Right box: " + rightBox);
				System.err.println("Tuple box: " + polygonBoundingBox);
			}
	    });
		
		try {
			tupleFile.processFile(MAX_ELEMENTS);
		} catch (IOException e) {
			logger.error("Got an IO-Exception while reading file", e);
			System.exit(-1);
		}
		
		return statistics;
	}
	
	/**
	 * Take a certain number of samples and generate a split position
	 * @param sampleSize
	 * @param numberOfElements 
	 * @return 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	protected double getSplit(final float sampleSize, 
			final long numberOfElements) throws ClassNotFoundException, IOException {
		
		final Set<Long> takenSamples = new HashSet<>();
		final Random random = new Random(System.currentTimeMillis());
		final List<BoundingBox> samples = new ArrayList<>();
		
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(format);
		try(
				final RandomAccessFile randomAccessFile = new RandomAccessFile(filename, "r");
			) {

			while(takenSamples.size() < sampleSize) {
				final long sampleId = Math.abs(random.nextLong()) % numberOfElements;
				
				if(takenSamples.contains(sampleId)) {
					continue;
				}
				
				// Line 1 is sample 0 in the file
				final long pos = fli.locateLine(sampleId + 1);
				randomAccessFile.seek(pos);
				final String line = randomAccessFile.readLine();
			    final Tuple tuple = tupleBuilder.buildTuple(Long.toString(sampleId), line);
			    
			    // E.g. Table header
			    if(tuple == null) {
			    	continue;
			    }
			    
				final BoundingBox boundingBox = tuple.getBoundingBox();
				samples.add(boundingBox);
				tupleDimension = boundingBox.getDimension();
				
				takenSamples.add(sampleId);

			}
		}
		
		samples.sort((b1, b2) -> Double.compare(b1.getCoordinateLow(0), b2.getCoordinateLow(0)));
		
		return samples.get(samples.size() / 2).getCoordinateLow(0);
	}
	
	/**
	 * Main * Main * Main
	 */
	public static void main(final String[] args) throws IOException {
		
		// Check parameter
		if(args.length != 2) {
			System.err.println("Usage: programm <filename> <format>");
			System.exit(-1);
		}
		
		final String filename = Objects.requireNonNull(args[0]);
		final String format = Objects.requireNonNull(args[1]);

		final DetermineSamplingSize determineSamplingSize = new DetermineSamplingSize(filename, format);
		determineSamplingSize.run();
	}

}

class ExperimentSeriesStatistics {
	protected long minDiff = Long.MAX_VALUE;
	protected long maxDiff = Long.MIN_VALUE;
	protected long avgDiffAll = 0;
	protected long totalElements = 0;
	protected int numberOfExperiments = 0;
	
	public void addExperiment(final ExperimentStatistics experimentStatistics) {
		minDiff = Math.min(minDiff, experimentStatistics.getDiff());
		maxDiff = Math.max(maxDiff, experimentStatistics.getDiff());
		avgDiffAll = avgDiffAll + experimentStatistics.getDiff();
		totalElements = experimentStatistics.getTotal();
		numberOfExperiments++;
	}
	
	public void printStatistics() {
		System.out.println("#Min diff\tMax diff\tAvg diff\tMin diff(%f)\tMax diff(%)\tAvg diff(%)");
		final long avgDiff = avgDiffAll / numberOfExperiments;
		final double pAvgDiff = (avgDiff / (double) totalElements) * 100.0;
		final double pMaxDiff = (maxDiff / (double) totalElements) * 100.0;
		final double pMinDiff = (minDiff / (double) totalElements) * 100.0;
		
		System.out.format("%d\t%d\t%d\t%f\t%f\t%f\n", minDiff, maxDiff, avgDiff, pMinDiff, pMaxDiff, pAvgDiff);
	}
}

class ExperimentStatistics {
	protected long left = 0;
	protected long right = 0;
	protected long total = 0;
	
	public void increaseLeft() {
		left++;
	}
	
	public void increaseRight() {
		right++;
	}
	
	public void increaseTotal() {
		total++;
	}
	
	public static void printExperientHeader() {
		System.out.println("#Experiment\tTotal\tLeft\tRight\tDiff\t% diff");
	}
	
	public void printExperimentResult(final int experiment) {
		final long diff = getDiff();
		final double pDiff = ((left - right) / (double) total) * 100.0;
		System.out.format("%d\t%d\t%d\t%d\t%d\t%f\n", experiment, total, left, right, diff, pDiff);
	}

	protected long getDiff() {
		return Math.abs(left - right);
	}
	
	public long getTotal() {
		return total;
	}

	@Override
	public String toString() {
		return String.format("Total %d, left %d, right %d, diff %d\n", total, left, right, getDiff());
	}
}
