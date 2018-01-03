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
	protected String filename;
	
	/**
	 * The input format
	 */
	protected final TupleBuilder tupleBuilder;
	
	/**
	 * The tuple callbacks
	 */
	protected final List<Consumer<? super Tuple>> callbacks;

	/** 
	 * The amount of processed lines
	 */
	protected long lineNumber;

	/**
	 * The last read file
	 */
	protected String fileLine;

	public TupleFileReader(final String filename, final String importFormat) {
		this.filename = filename;
		this.tupleBuilder = TupleBuilderFactory.getBuilderForFormat(importFormat);
		this.callbacks = new ArrayList<>();
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
}
