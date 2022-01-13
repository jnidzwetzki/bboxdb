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
package org.bboxdb.experiments.conference;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.tools.TupleFileReader;

public class TestBaselineSelect implements Runnable {

	/**
	 * The file to read
	 */
	private File file;

	/**
	 * The format to parse
	 */
	private String format;

	/**
	 * The range to query
	 */
	private Hyperrectangle range;

	public TestBaselineSelect(final File file, final String format, final Hyperrectangle range) {
		this.file = file;
		this.format = format;
		this.range = range;
	}

	public static void main(final String[] args) {
		if(args.length != 3) {
			System.err.println("Usage: <File> <Format> <Range>");
			System.exit(-1);
		}

		final File file = new File(args[0]);

		if(! file.exists()) {
			System.err.println("File " + file + " does not exist");
			System.exit(-1);
		}

		final Hyperrectangle range = Hyperrectangle.fromString(args[2]);

		final TestBaselineSelect baselineSelect = new TestBaselineSelect(file, args[1], range);
		baselineSelect.run();
	}

	@Override
	public void run() {
		try {
			final AtomicLong tupleFound = new AtomicLong(0);

			final TupleFileReader reader = new TupleFileReader(file.getAbsolutePath(), format);
			reader.addTupleListener((t) -> {
				if(t.getBoundingBox().intersects(range)) {
					tupleFound.incrementAndGet();
				}
			});

			reader.processFile();

			System.out.format("Found %d tuples%n", tupleFound.get());
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
