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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.bboxdb.commons.math.DoubleInterval;
import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.commons.math.Hyperrectangle;

public class ConvertGeoJSONToBoundingBox implements Runnable {

	/**
	 * The input filename
	 */
	private final String inputFile;
	
	/**
	 * The output filename
	 */
	private final String outputFile;

	public ConvertGeoJSONToBoundingBox(final String inputFile, final String outputFile) {
		this.inputFile = inputFile;
		this.outputFile = outputFile;
	}

	@Override
	public void run() {
		
		final File outputFileHandle = new File(outputFile);
		if(outputFileHandle.exists()) {
			System.err.format("File %s exists, exiting%n", outputFile);
			System.exit(-1);
		}
		
		try (
				final Stream<String> stream = Files.lines(Paths.get(inputFile));
				final BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileHandle));
			) {
			stream.forEach(l -> handleLine(l, bw));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * Handle a new line
	 * @param line
	 * @param writer
	 * @return
	 */
	private void handleLine(final String line, final Writer writer) {
		try {
			final GeoJsonPolygon polygon = GeoJsonPolygon.fromGeoJson(line);
			final Hyperrectangle boundingBox = polygon.getBoundingBox();
			final int dimension = boundingBox.getDimension();
			
			assert dimension == 2 : "Dimension of bodunig box is not 2: " + dimension;
			
			final DoubleInterval interval0 = boundingBox.getIntervalForDimension(0);
			final DoubleInterval interval1 = boundingBox.getIntervalForDimension(1);

			final GeoJsonPolygon boundingBoxPolygon = new GeoJsonPolygon(polygon.getId());
			
			boundingBoxPolygon.addPoint(interval0.getBegin(), interval1.getBegin());
			boundingBoxPolygon.addPoint(interval0.getBegin(), interval1.getEnd());
			boundingBoxPolygon.addPoint(interval0.getEnd(), interval1.getEnd());
			boundingBoxPolygon.addPoint(interval0.getEnd(), interval1.getBegin());
			boundingBoxPolygon.addPoint(interval0.getBegin(), interval1.getBegin());

			// Copy properties
			final Map<String, String> properties = polygon.getProperties();
			
			for(final Entry<String, String> entry : properties.entrySet()) {
				final String key = entry.getKey();
				final String value = entry.getValue();
				boundingBoxPolygon.addProperty(key, value);
			}
			
			writer.write(boundingBoxPolygon.toGeoJson());
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * Main * Main * Main
	 */
	public static void main(final String[] args) throws IOException {

		// Check parameter
		if(args.length != 2) {
			System.err.println("Usage: programm <input filename> <output filename>");
			System.exit(-1);
		}

		final String inputFile = Objects.requireNonNull(args[0]);
		final String outputFile = Objects.requireNonNull(args[1]);
	
		final ConvertGeoJSONToBoundingBox converter = new ConvertGeoJSONToBoundingBox(
				inputFile, outputFile);

		converter.run();
	}
}
