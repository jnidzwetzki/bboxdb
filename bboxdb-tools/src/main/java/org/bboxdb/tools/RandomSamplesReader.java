/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
package org.bboxdb.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.converter.tuple.TupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilderFactory;

public class RandomSamplesReader {
	
	/**
	 * Read samples random from files
	 * @param filename
	 * @param format
	 * @param samples
	 * @return
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static List<Hyperrectangle> readSamplesRandom(final String filename, final String format, 
			final double samplingPercent) throws IOException, FileNotFoundException {
		
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(format);
		final Set<Long> sampleLines = new HashSet<>();
		final List<Hyperrectangle> samples = new ArrayList<>();

		try(
				final RandomAccessFile randomAccessFile = new RandomAccessFile(filename, "r");
				final FileLineIndex fli = new FileLineIndex(filename);
		) {
			System.out.format("Indexing %s%n", filename);
			fli.indexFile();
			final long indexedLines = fli.getIndexedLines();
			final long neededSamples = (long) (samplingPercent / 100 * indexedLines);
			System.out.format("Indexing %s done (%d elements, taking %d samples) %n", 
					filename, fli.getIndexedLines(), neededSamples);

			while(sampleLines.size() < neededSamples) {
				final long lineNumber = ThreadLocalRandom.current().nextLong(indexedLines);

				if(! sampleLines.add(lineNumber)) {
					continue;
				}
			}
			
			// Access the disk pages sequentially
			final List<Long> sampleLinesOrdered = new ArrayList<>(sampleLines);
			Collections.sort(sampleLinesOrdered);
			
			for(final long lineNumber : sampleLinesOrdered) {
				
				Tuple tuple = null;
				int offset = 0;
				
				// Some tuple (e.g., ADS-B data) need some line for one tuple
				do {
					final String lineString = getLine(randomAccessFile, fli, lineNumber + offset);
					tuple = tupleBuilder.buildTuple(lineString, Long.toString(lineNumber + offset));
					offset++;
				} while(tuple == null);
			    
			    samples.add(tuple.getBoundingBox());
			}
		}
		
		return samples;
	}

	/**
	 * Get the given line
	 * @param randomAccessFile
	 * @param fli
	 * @param lineNumber
	 * @return
	 * @throws IOException
	 */
	private static String getLine(final RandomAccessFile randomAccessFile, final FileLineIndex fli,
			final long lineNumber) throws IOException {
		
		final long pos = fli.locateLine(lineNumber);
		randomAccessFile.seek(pos);
		final String lineString = randomAccessFile.readLine();
		return lineString;
	}
}
