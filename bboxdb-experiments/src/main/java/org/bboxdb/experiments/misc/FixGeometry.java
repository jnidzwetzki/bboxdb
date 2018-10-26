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
package org.bboxdb.experiments.misc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.bboxdb.tools.TupleFileReader;
import org.bboxdb.tools.converter.osm.util.Polygon;
import org.bboxdb.tools.converter.tuple.TupleBuilderFactory;

public class FixGeometry implements Runnable {

	/**
	 * The input file
	 */
	private String input;

	/**
	 * The output file
	 */
	private String output;

	public FixGeometry(final String input, final String output) {
		this.input = input;
		this.output = output;
	}

	@Override
	public void run() {

		try(
				final BufferedWriter writer = new BufferedWriter(new FileWriter(new File(output)))
				) {

			final TupleFileReader tupleFile = new TupleFileReader(input, TupleBuilderFactory.Name.GEOJSON);
			tupleFile.addTupleListener((t) -> {
				try {
					final Polygon polygon = Polygon.fromGeoJson(new String(t.getDataBytes()));
					final String json = polygon.toGeoJson(true);
					writer.write(json);
					writer.write("\n");
				} catch (IOException e) {
					throw new RuntimeException();
				}
			});

			tupleFile.processFile();
		} catch (IOException e) {
			throw new RuntimeException();
		}
	}

	public static void main(final String[] args) {
		if(args.length != 2) {
			System.err.println("Usage <Input> <Output>");
			System.exit(1);
		}

		final String input = args[0];
		final String output = args[1];

		final File inputFile = new File(input);
		final File outputFile = new File(output);

		if(! inputFile.exists()) {
			System.err.println("Input file does not exist");
			System.exit(1);
		}

		if(outputFile.exists()) {
			System.err.println("Outpit file does already exist");
			System.exit(1);
		}

		final FixGeometry fixGeometry = new FixGeometry(input, output);
		fixGeometry.run();
	}
}
