/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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
package org.bboxdb.experiments.misc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.FileLineIndex;
import org.bboxdb.tools.converter.tuple.TupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilderFactory;

public class DetermineRangeQueryBoxes implements Runnable {

	/**
	 * The filename to read
	 */
	private final String filename;

	/**
	 * The format to parse
	 */
	private final String format;

	/**
	 * The amount of queries
	 */
	private final long queries;

	/**
	 * The increase factor
	 */
	private final long factor;

	/**
	 * The file line index
	 */
	private FileLineIndex fli;

	public DetermineRangeQueryBoxes(final String filename, final String format,
			final long queries, final long factor) {

				this.filename = filename;
				this.format = format;
				this.queries = queries;
				this.factor = factor;
	}

	@Override
	public void run() {
		try {
			final Set<Long> takenSamples = new HashSet<>();
			final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(format);

			System.out.format("Indexing %s%n", filename);
			fli = new FileLineIndex(filename);
			fli.indexFile();
			System.out.format("Indexing %s done%n", filename);

			try(
					final RandomAccessFile randomAccessFile = new RandomAccessFile(filename, "r");
			) {
				while(takenSamples.size() < queries) {
					final long sampleId = ThreadLocalRandom.current().nextLong(fli.getIndexedLines());

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

					final Hyperrectangle boundingBox = tuple.getBoundingBox();
					final Hyperrectangle enlargedBox = boundingBox.enlargeByFactor(factor);
					System.out.print(takenSamples.size() + " " + boundingBox.toCompactString() + " ");
					System.out.println(enlargedBox.toCompactString());

					takenSamples.add(sampleId);
				}
			}
		} catch (Exception e) {
			System.err.println("Got exception: ");
			e.printStackTrace();
		}
	}

	/**
	 * Main * Main * Main
	 */
	public static void main(final String[] args) throws IOException {

		// Check parameter
		if(args.length != 4) {
			System.err.println("Usage: programm <filename> <format> <queries> <factor>");
			System.exit(-1);
		}

		final String filename = Objects.requireNonNull(args[0]);
		final String format = Objects.requireNonNull(args[1]);
		final long queries = MathUtil.tryParseLongOrExit(args[2]);
		final long factor = MathUtil.tryParseLongOrExit(args[3]);

		final DetermineRangeQueryBoxes program = new DetermineRangeQueryBoxes(
				filename, format, queries, factor);

		program.run();
	}
}
