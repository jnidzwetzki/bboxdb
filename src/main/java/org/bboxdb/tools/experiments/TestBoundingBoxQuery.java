/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.tools.experiments;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;

public class TestBoundingBoxQuery implements Runnable {

	/**
	 * The file to import
	 */
	protected final String filename;
	
	/**
	 * The format of the input file
	 */
	protected String format;

	/**
	 * The sampling size
	 */
	protected final static double SAMPLING_SIZE = 1d;

	/**
	 * The random for our samples
	 */
	protected final Random random;

	/**
	 * The dimension of the input data
	 */
	protected int dataDimension = -1;
	
	/**
	 * The tablename to query
	 */
	protected final String tablename;
	
	public TestBoundingBoxQuery(final String filename, final String format, final String tablename) {
		this.filename = filename;
		this.format = format;
		this.tablename = tablename;
		this.random = new Random(System.currentTimeMillis());
	}
	
	@Override
	public void run() {
		System.out.format("Reading %s\n", filename);
		final List<Double> experimentSize = Arrays.asList(0.1, 0.2, 0.5, 1.0);
		experimentSize.forEach(e -> runExperiment(e));
	}

	/**
	 * Run the experiment with the max dimension soze
	 * @param sampleSize
	 * @throws IOException 
	 */
	protected void runExperiment(final double maxDimensionSize) {
		System.out.println("# Simulating with max dimension size: " + maxDimensionSize);
	
	}

	
	/**
	 * Main * Main * Main
	 */
	public static void main(final String[] args) throws IOException {
		
		// Check parameter
		if(args.length != 3) {
			System.err.println("Usage: programm <filename> <format> <table>");
			System.exit(-1);
		}
		
		final String filename = Objects.requireNonNull(args[0]);
		final String format = Objects.requireNonNull(args[1]);
		final String table = Objects.requireNonNull(args[2]);
		
		final TestBoundingBoxQuery testSplit = new TestBoundingBoxQuery(filename, format, table);
		testSplit.run();
	}

}
