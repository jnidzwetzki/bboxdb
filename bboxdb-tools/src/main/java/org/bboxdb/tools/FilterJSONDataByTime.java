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
package org.bboxdb.tools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.GeoJsonPolygon;

public class FilterJSONDataByTime implements Runnable {
	
	/**
	 * The file to read
	 */
	private final File file;
	
	/**
	 * The begin timestamp
	 */
	private final long begin;
	
	/**
	 * The end timestamp
	 */
	private final long end;

	public FilterJSONDataByTime(final File file, final long begin, final long end) {
		this.file = file;
		this.begin = begin;
		this.end = end;
	}

	@Override
	public void run() {
		try (final Stream<String> stream = Files.lines(Paths.get(file.toURI()))) {
			stream.forEach(l -> handleline(l));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Handle the file line by line
	 * @param line
	 */
	private void handleline(final String line) {
		final GeoJsonPolygon polygon = GeoJsonPolygon.fromGeoJson(line);
		final String timestampstring = polygon.getProperties().get("Timestamp");
		final long timestamp = MathUtil.tryParseLongOrExit(timestampstring);
		
		if(timestamp > begin && timestamp < end) {
			System.out.println(line);
		}
	}

	public static void main(final String[] args) {
		
		if(args.length != 3) {
			System.err.println("Usage: <Class> <Filename> <Begin> <End>");
			System.exit(-1);
		}
		
		final File file = new File(args[0]);
		
		if(! file.isFile()) {
			System.err.println("Unable to open: " + file);
			System.exit(-1);
		}
		
		final long begin = MathUtil.tryParseLongOrExit(args[1]);
		final long end = MathUtil.tryParseLongOrExit(args[2]);
		
		final FilterJSONDataByTime filter = new FilterJSONDataByTime(file, begin, end);
		filter.run();
	}
}
