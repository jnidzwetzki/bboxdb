/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.converter.tuple.TupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilderFactory;

public class DetermineDistributionStateSize extends AbstractStateSize implements Runnable {

	/** 
	 * The amount of processed lines
	 */
	private long lineNumber;

	/**
	 * The last read file
	 */
	private String fileLine;
	
	/**
	 * The distribution state
	 */
	private final Map<String, Long> distributionState;
	
	/**
	 * Already seen elements, used to calculate the error
	 */
	private final Set<String> alreadySeenKeys;
	
	/**
	 * The keys that were seen but not in state
	 */
	private long seenButNotInState;
	
	/**
	 * Last state dumped
	 */
	protected boolean lastStateDumped;
	
	public DetermineDistributionStateSize(final File inputFile, final TupleBuilder tupleFactory) {
		super(inputFile, tupleFactory);
		this.distributionState = new HashMap<>();
		this.alreadySeenKeys = new HashSet<>();
		this.lineNumber = 0;
		this.seenButNotInState = 0;
		this.fileLine = null;
		this.lastStateDumped = false;
	}


	@Override
	public void run() {
		
		System.out.println("#########################");
		System.out.println("## Input file: " + inputFile);
		
		final long linesInInput = determineLinesInInput();
		
		System.out.println("## Lines in input: " + linesInInput);
		
		final long stateAfterLines = linesInInput / 20;
		
		System.out.println("## State after lines: " + stateAfterLines);
		System.out.println("#########################");
		
		final List<Integer> invalidations = Arrays.asList(new Integer[] {0, 1, 5, 10, 15, 20, 25, 50, 75, 100, 150, 200, 250, 500, 750, 1000});
		
		for(final Integer invalidateAfterGenerations : invalidations) {
			
			distributionState.clear();
			alreadySeenKeys.clear();
			lastWatermarkGenerated = 0;
			seenButNotInState = 0;
			lastStateDumped = false;
			
			System.out.println("#########################");
			System.out.println("## Invaliate after: " + invalidateAfterGenerations);
			System.out.println("## % input \t entries \t size in byte");
			
			try(final Stream<String> fileStream = Files.lines(Paths.get(inputFile.getAbsolutePath()))) {
				
				lineNumber = 1;
				Tuple lastTuple = null;
				
				long watermarkGeneration = 0;
				
				for (final Iterator<String> iterator = fileStream.iterator(); iterator.hasNext();) {
					fileLine = iterator.next();
					final Tuple tuple = tupleBuilder.buildTuple(fileLine);
					
					if(tuple != null) {
						
						final boolean watermarkCreated = isWatermarkCreated(lastTuple, tuple);
				
						final String tupleKey = tuple.getKey();
						if(! distributionState.containsKey(tupleKey)) {
							if(alreadySeenKeys.contains(tupleKey)) {
								seenButNotInState++;
							} else {
								alreadySeenKeys.add(tupleKey);
							}
						}
						
						distributionState.put(tupleKey, watermarkGeneration);
						
						if(watermarkCreated) {
							cleanupDistributionStructure(watermarkGeneration, invalidateAfterGenerations);
							watermarkGeneration++;
						}
						
						lastTuple = tuple;
					}
					
					if(lineNumber % (stateAfterLines) == 0) {
						final double roundPercent = dumpStatistics(linesInInput);
						
						if(roundPercent == 100) {
							lastStateDumped = true;
						}
					}
					
					lineNumber++;
				}
				
				if(! lastStateDumped) {
					dumpStatistics(linesInInput);
				}
				
				System.out.println("# Already seen but not in state: " + seenButNotInState);
				System.out.println("#########################\n\n");

			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		
	}

	/**
	 * Dump the statistics
	 * @param linesInInput
	 * @return
	 */
	private double dumpStatistics(final long linesInInput) {
		final double percent = ((double) lineNumber / (double) linesInInput) * 100.0;
		
		final double roundPercent = MathUtil.round(percent, 0);
		
		final long stateSize = determineStateSize();
		System.out.println(roundPercent + "\t" + distributionState.size() + "\t" + stateSize);
		return roundPercent;
	}

	/**
	 * Cleanup distribution structure
	 * @param watermarkGeneration
	 * @param invalidateAfterGenerations
	 */
	private void cleanupDistributionStructure(final long watermarkGeneration, final long invalidateAfterGenerations) {
		
		if(invalidateAfterGenerations == 0) {
			return;
		}
		
        final List<Entry<String, Long>> elementsToRemove = distributionState
                .entrySet()
                .stream()
                .filter(e -> (e.getValue() <= watermarkGeneration - invalidateAfterGenerations))
                .collect(Collectors.toList());
		
		if(DEBUG) {
			System.out.println("Current generation is: " + watermarkGeneration);
			System.out.println("Removing: " + elementsToRemove);
		}
		
		for(final Entry<String, Long> entry : elementsToRemove) {
			distributionState.remove(entry.getKey());
		}
	}


	/**
	 * Determine the size of the distribution state
	 * @return
	 */
	private long determineStateSize() {
		long size = 0;
		
		for(final Map.Entry<String, Long> entry : distributionState.entrySet()) {
			// Size of entry
			size = size + entry.getKey().getBytes().length; 	
			
			// Size of two 64 bit long values 
			// I.e., the distribution region and the watermark counter
			size = size + (2 * 8);
		}
		
		return size;
	}


	/***
	 * Main * Main * Main * Main * Main
	 */
	public static void main(final String[] args) {
		
		if(args.length != 2) {
			System.err.println("Usage: <Class> <Filename> <Format>");
			System.exit(1);
		}
		
		final String filename = args[0];
		final String format = args[1];
		
		// Check file
		final File inputFile = new File(filename);
		if(! inputFile.isFile()) {
			System.err.println("Unable to open file: " + filename);
			System.exit(1);
		}
		
		final TupleBuilder tupleFactory = TupleBuilderFactory.getBuilderForFormat(format);
		
		final DetermineDistributionStateSize determineDistributionStateSize = new DetermineDistributionStateSize(inputFile, tupleFactory);
		determineDistributionStateSize.run();
	}

}
