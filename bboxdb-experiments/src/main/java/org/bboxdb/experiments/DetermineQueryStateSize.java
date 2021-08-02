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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.commons.math.HyperrectangleHelper;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.converter.tuple.TupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilderFactory;
import org.bboxdb.tools.helper.RandomQueryRangeGenerator;

public class DetermineQueryStateSize extends AbstractStateSize implements Runnable {

	/** 
	 * The amount of processed lines
	 */
	private long lineNumber;

	/**
	 * The last read file
	 */
	private String fileLine;
	
	/**
	 * The keys that were seen but not in state
	 */
	private long seenButNotInState;
	
	/**
	 * Processed elements
	 */
	private long processedElements;

	/**
	 * The queries
	 */
	private final List<Query> queries;
	
	/**
	 * Last state dumped
	 */
	protected boolean lastStateDumped;
	
	class Query {
		
		/**
		 * The query rectangle
		 */
		private final Hyperrectangle queryRectangle;
		
		/**
		 * The distribution state
		 */
		private final Map<String, Long> queryState;
		
		/**
		 * Already seen elements, used to calculate the error
		 */
		private final Set<String> alreadySeenKeys;

		public Query(final Hyperrectangle queryRectangle) {
			this.queryRectangle = queryRectangle;
			this.queryState = new HashMap<>();
			this.alreadySeenKeys = new HashSet<>();
		}
		
		public Hyperrectangle getQueryRectangle() {
			return queryRectangle;
		}
		
		public Map<String, Long> getQueryState() {
			return queryState;
		}
		
		public Set<String> getAlreadySeenKeys() {
			return alreadySeenKeys;
		}
	}
	
	public DetermineQueryStateSize(final File inputFile, final TupleBuilder tupleFactory, 
			final List<Hyperrectangle> ranges) {
		
		super(inputFile, tupleFactory);
		this.lineNumber = 0;
		this.seenButNotInState = 0;
		this.processedElements = 0;
		this.fileLine = null;
		this.lastStateDumped = false;
		
		this.queries = new ArrayList<>();
		
		for(final Hyperrectangle range : ranges) {
			queries.add(new Query(range));
		}
		
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
			
			queries.forEach(q -> q.getQueryState().clear());
			queries.forEach(q -> q.getAlreadySeenKeys().clear());
			lastWatermarkGenerated = 0;
			seenButNotInState = 0;
			processedElements = 0;
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
						
						for(final Query query : queries) {
							final String tupleKey = tuple.getKey();
						
							if(query.getQueryRectangle().intersects(tuple.getBoundingBox())) {
								
								if(query.getAlreadySeenKeys().contains(tupleKey) && ! query.getQueryState().containsKey(tupleKey)) {
									seenButNotInState++;
								}
								
								query.getAlreadySeenKeys().add(tupleKey);
								query.getQueryState().put(tupleKey, watermarkGeneration);
								processedElements++;
							} else {
								query.getAlreadySeenKeys().remove(tupleKey);
								query.getQueryState().remove(tupleKey);
							}
						}
						
						
						final boolean watermarkCreated = isWatermarkCreated(lastTuple, tuple);

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
				
				final double errorPercentage = (((double) seenButNotInState / (double) processedElements) * 100.0);
				
				System.out.println("# Already seen but not in state: " + seenButNotInState + " / processed " 
							+ processedElements + " / " + MathUtil.round(errorPercentage, 4) + " %");
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
		System.out.println(roundPercent + "\t" + determineStateEntries() + "\t" + stateSize);
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
		
		for(final Query query : queries) {
		
	        final List<Entry<String, Long>> elementsToRemove = query.getQueryState()
	                .entrySet()
	                .stream()
	                .filter(e -> (e.getValue() <= watermarkGeneration - invalidateAfterGenerations))
	                .collect(Collectors.toList());
			
			if(DEBUG) {
				System.out.println("Current generation is: " + watermarkGeneration);
				System.out.println("Removing: " + elementsToRemove);
			}
			
			for(final Entry<String, Long> entry : elementsToRemove) {
				query.getQueryState().remove(entry.getKey());
			}
		}
	}


	/**
	 * Determine the size of the distribution state
	 * @return
	 */
	private long determineStateSize() {
		long size = 0;
		
		for(final Query query : queries) {
			for(final Map.Entry<String, Long> entry : query.getQueryState().entrySet()) {
				
				// Size of entry
				size = size + entry.getKey().getBytes().length; 	
				
				// Size of one 64 bit long value
				// I.e., the watermark counter
				size = size + (1 * 8);
			}
		}

		return size;
	}

	/**
	 * Determine the state entries
	 * @return
	 */
	private long determineStateEntries() {
		return queries.stream().mapToInt(q -> q.getQueryState().size()).sum();
	}
	

	/***
	 * Main * Main * Main * Main * Main
	 */
	public static void main(final String[] args) {
		
		if(args.length != 5) {
			System.err.println("Usage: <Class> <Filename> <Format> <Query Rectangle> <Coverage> <Queries>");
			System.exit(1);
		}
		
		final String filename = args[0];
		final String format = args[1];
		final String rangeString = args[2];
		final String percentageString = args[3];
		final String parallelQueriesString = args[4];
		
		// Check file
		final File inputFile = new File(filename);
		if(! inputFile.isFile()) {
			System.err.println("Unable to open file: " + filename);
			System.exit(1);
		}
		
		final TupleBuilder tupleFactory = TupleBuilderFactory.getBuilderForFormat(format);
		
		final Optional<Hyperrectangle> range = HyperrectangleHelper.parseBBox(rangeString);
		
		if(! range.isPresent()) {
			System.err.println("Unable to parse as bounding box: " + rangeString);
		}
		
		final double percentage = MathUtil.tryParseDoubleOrExit(percentageString, () -> "Unable to parse: " + percentageString);
		final double parallelQueries = MathUtil.tryParseDoubleOrExit(parallelQueriesString, () -> "Unable to parse: " + parallelQueriesString);
		
		final List<Hyperrectangle> ranges = new ArrayList<>();
	
		for(int i = 0; i < parallelQueries; i++) {
			final Hyperrectangle queryRectangle = RandomQueryRangeGenerator.getRandomQueryRange(range.get(), percentage);
			ranges.add(queryRectangle);
		}
		
		final DetermineQueryStateSize determineDistributionStateSize = new DetermineQueryStateSize(inputFile, tupleFactory, ranges);
		determineDistributionStateSize.run();
	}

}
