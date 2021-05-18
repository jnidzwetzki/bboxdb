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
import java.util.Optional;
import java.util.stream.Stream;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.commons.math.HyperrectangleHelper;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.converter.tuple.TupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilderFactory;

public class FilterJSONDataByBBox implements Runnable {
	
	/**
	 * The file to read
	 */
	private final File file;
	
	/**
	 * The boudnign box
	 */
	private final Hyperrectangle boundingBox;

	/**
	 * The tuple builder
	 */
	private final TupleBuilder tupleBuilder;

	public FilterJSONDataByBBox(final File file, final String importFormat, final Hyperrectangle boundingBox) {
		this.file = file;
		this.boundingBox = boundingBox;
		this.tupleBuilder = TupleBuilderFactory.getBuilderForFormat(importFormat);
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
		final Tuple tuple = tupleBuilder.buildTuple(line);

		if(tuple == null) {
			System.err.println("Tuple is null: " + line);
			return;
		}
		
		if(boundingBox.intersects(tuple.getBoundingBox())) {
 			System.out.println(line);
		}
	}

	public static void main(final String[] args) {
		
		if(args.length != 3) {
			System.err.println("Usage: <Class> <Filename> <InputFormat> <BBox>");
			System.exit(-1);
		}
		
		final File file = new File(args[0]);
		
		if(! file.isFile()) {
			System.err.println("Unable to open: " + file);
			System.exit(-1);
		}
		
		final String inputFormat = args[1];
		
		final Optional<Hyperrectangle> boundingBox = HyperrectangleHelper.parseBBox(args[2]);
		
		if(! boundingBox.isPresent()) {
			System.err.println("Invalid bounding box: " + args[2]);
			System.exit(-1);
		}
		
		final FilterJSONDataByBBox filter = new FilterJSONDataByBBox(file, inputFormat, boundingBox.get());
		filter.run();
	}
}
