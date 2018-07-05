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
package org.bboxdb.tools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.converter.tuple.TupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilderFactory;

public class TupleFileReader {

	/**
	 * The filename to read
	 */
	private String filename;
	
	/**
	 * The input format
	 */
	private final TupleBuilder tupleBuilder;
	
	/**
	 * The bounding box padding
	 */
	private final double boxPadding;
	
	/**
	 * The tuple callbacks
	 */
	private final List<Consumer<? super Tuple>> callbacks;

	/** 
	 * The amount of processed lines
	 */
	private long lineNumber;

	/**
	 * The last read file
	 */
	private String fileLine;
	
	/**
	 * The default bounding box padding
	 */
	private final static double DEFAULT_BOX_PADDING = 0.0;

	public TupleFileReader(final String filename, final String importFormat) {
		this(filename, importFormat, DEFAULT_BOX_PADDING);
	}
	
	public TupleFileReader(final String filename, final String importFormat, final double boxPadding) {
		this.filename = filename;
		this.boxPadding = boxPadding;
		this.tupleBuilder = TupleBuilderFactory.getBuilderForFormat(importFormat);
		this.callbacks = new ArrayList<>();
		
		tupleBuilder.setPadding(boxPadding);
	}
	
	/**
	 * Add a listener for each processed tuple
	 * @param listener
	 */
	public void addTupleListener(final Consumer<? super Tuple> listener) {
		callbacks.add(listener);
	}
	
	/**
	 * Process all lines of the file
	 * @throws IOException
	 */
	public void processFile() throws IOException {
		processFile(Long.MAX_VALUE);
	}
	
	/**
	 * Read the file with a maximal number of lines
	 * @throws IOException 
	 */
	public void processFile(final long maxLines) throws IOException {
		final File file = new File(filename);
		if(! file.exists()) {
			throw new IOException("Unable to open file: " + file);
		}
		
		try(final Stream<String> fileStream = Files.lines(Paths.get(filename))) {
			lineNumber = 1;
			
			for (final Iterator<String> iterator = fileStream.iterator(); iterator.hasNext();) {
				fileLine = iterator.next();
				final Tuple tuple = tupleBuilder.buildTuple(Long.toString(lineNumber), fileLine);
				
				if(tuple != null) {
					callbacks.forEach(c -> c.accept(tuple));
				}
				
				lineNumber++;
				
				if(lineNumber > maxLines) {
					break;
				}
			}
		}
	}
	
	/**
	 * Get the amount of processed tuples
	 * @return
	 */
	public long getProcessedLines() {
		return lineNumber;
	}
	
	/**
	 * The last read line
	 * @return
	 */
	public String getLastReadLine() {
		return fileLine;
	}
	
	/**
	 * Get the bounding box padding
	 * @return
	 */
	public double getBoxPadding() {
		return boxPadding;
	}
}
