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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.FileLineIndex;
import org.bboxdb.tools.converter.tuple.ADSBTupleBuilder2D;
import org.bboxdb.tools.converter.tuple.ADSBTupleBuilder3D;
import org.bboxdb.tools.converter.tuple.BerlinModTupleBuilder;
import org.bboxdb.tools.converter.tuple.GeoJSONTupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilderFactory;

public class DetermineDistributionStateSize implements Runnable {

	/**
	 * The input file
	 */
	private final File inputFile;

	/**
	 * The tuple factory
	 */
	private final TupleBuilder tupleBuilder;
	
	/** 
	 * The amount of processed lines
	 */
	private long lineNumber;

	/**
	 * The last read file
	 */
	private String fileLine;
	
	/**
	 * The time the lastWatermark is generated
	 */
	private long lastWatermarkGenerated;

	/**
	 * The distribution state
	 */
	private final Map<String, Long> distributionState;
	
	public DetermineDistributionStateSize(final File inputFile, final TupleBuilder tupleFactory) {
		this.inputFile = inputFile;
		this.tupleBuilder = tupleFactory;
		this.distributionState = new HashMap<>();
		this.lineNumber = 0;
		this.lastWatermarkGenerated = 0;
		this.fileLine = null;
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
		
		for(int invalidateAfterGenerations = 0; invalidateAfterGenerations < 10; invalidateAfterGenerations++) {
			
			System.out.println("#########################");
			System.out.println("## Invaliate after: " + invalidateAfterGenerations);
			System.out.println("## % input \t entries \t size in byte");
			
			try(final Stream<String> fileStream = Files.lines(Paths.get(inputFile.getAbsolutePath()))) {
				
				lineNumber = 1;
				Tuple lastTuple = null;
				
				long watermarkGeneration = 0;
				
				for (final Iterator<String> iterator = fileStream.iterator(); iterator.hasNext();) {
					fileLine = iterator.next();
					final Tuple tuple = tupleBuilder.buildTuple(fileLine, Long.toString(lineNumber));
					
					if(tuple != null) {
						
						final boolean watermarkCreated = isWatermarkCreated(lastTuple, tuple);
				
						distributionState.put(tuple.getKey(), watermarkGeneration);
						
						if(watermarkCreated) {
							watermarkGeneration++;
							cleanupDistributionStructure(watermarkGeneration, invalidateAfterGenerations);
						}
						
						lastTuple = tuple;
					}
					
					if(lineNumber % (stateAfterLines) == 0) {
						final double percent = ((double) lineNumber / (double) linesInInput) * 100.0;
						
						final double roundPercent = MathUtil.round(percent, 0);
						
						final long stateSize = determineStateSize();
						System.out.println(roundPercent + "\t" + distributionState.size() + "\t" + stateSize);
					}
					
					lineNumber++;
				}
				
				System.out.println("#########################\n\n");

			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		
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
		
		distributionState
			.entrySet()
			.removeIf(e -> watermarkGeneration - invalidateAfterGenerations <= e.getValue());
	}


	/**
	 * Is a watermark generated
	 * @param lastTuple
	 * @param tuple
	 * @return
	 */
	private boolean isWatermarkCreated(final Tuple lastTuple, final Tuple tuple) {
		
		if(lastTuple == null) {
			return false;
		}
		

		if(tupleBuilder instanceof ADSBTupleBuilder2D || tupleBuilder instanceof ADSBTupleBuilder3D) {
			final GeoJsonPolygon polygonOld = GeoJsonPolygon.fromGeoJson(new String(tuple.getDataBytes()));			
			final GeoJsonPolygon polygonNew = GeoJsonPolygon.fromGeoJson(new String(tuple.getDataBytes()));			

			return (polygonOld.getId() > polygonNew.getId());
		}
		
		if(tupleBuilder instanceof BerlinModTupleBuilder) {
			
			final long newTimestamp = tuple.getVersionTimestamp();
			
			if(lastWatermarkGenerated == 0) {
				lastWatermarkGenerated = newTimestamp;
				return false;
			}
			
			if(lastWatermarkGenerated + 60 < newTimestamp) {
				lastWatermarkGenerated = newTimestamp;
				return true;
			}
			
			return false;
		}
		
		if(tupleBuilder instanceof GeoJSONTupleBuilder) {

			final GeoJsonPolygon polygonNew = GeoJsonPolygon.fromGeoJson(new String(tuple.getDataBytes()));			
			final String newTimestampString = polygonNew.getProperties().getOrDefault("TimestampParsed", "-1");
			final long newTimestamp = MathUtil.tryParseLongOrExit(newTimestampString, () -> "Unable to parse: " + newTimestampString);
			
			if(lastWatermarkGenerated == 0) {
				lastWatermarkGenerated = newTimestamp;
				return false;
			}
			
			if(lastWatermarkGenerated + 60 < newTimestamp) {
				lastWatermarkGenerated = newTimestamp;
				return true;
			}
			
			return false;
		}
		
		System.out.println("Unsupported tuple builder: " + tupleBuilder);
		System.exit(1);
		return false;
	}


	/**
	 * Determine the size of the distribution state
	 * @return
	 */
	private long determineStateSize() {
		long size = 0;
		
		for(Map.Entry<String, Long> entry : distributionState.entrySet()) {
			size = size + entry.getKey().getBytes().length;
			size = size + 8;
		}
		
		return size;
	}


	/**
	 * Determine the lines of the file
	 * @return
	 * @throws IOException 
	 */
	private long determineLinesInInput() {
		final String filename = inputFile.getAbsolutePath();
		
		try(FileLineIndex fli = new FileLineIndex(filename)) {
			System.out.format("Indexing %s%n", filename);
			fli.indexFile();
			return fli.getIndexedLines();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
			return -1;
		}
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
