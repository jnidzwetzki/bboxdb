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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.CellGrid;
import org.bboxdb.util.MathUtil;
import org.bboxdb.util.TupleFileReader;

public class TestFixedGrid implements Runnable {

	/**
	 * The file to import
	 */
	protected final String filename;
	
	/**
	 * The format of the input file
	 */
	protected String format;
	
	/**
	 * The bounding boxes
	 */
	protected CellGrid cellGrid;

	/**
	 * The cell size
	 */
	protected double cellsPerDimension;
	
	/**
	 * The number of storing nodes
	 */
	protected int NODES = 385;
	
	public TestFixedGrid(final String filename, final String format, final double cellsPerDimension) {
		this.filename = filename;
		this.format = format;
		this.cellsPerDimension = cellsPerDimension;
	}
	
	@Override
	public void run() {
		System.out.format("Reading %s\n", filename);
		final BoundingBox boundingBox = determineBoundingBox();
		this.cellGrid = new CellGrid(boundingBox, cellsPerDimension);
		
		runExperiment();
	}

	/**
	 * Run this experiment
	 */
	protected void runExperiment() {
		final TupleFileReader tupleFile = new TupleFileReader(filename, format);
		final Map<BoundingBox, Integer> bboxes = new HashMap<>();
		
		tupleFile.addTupleListener(t -> {
			final Set<BoundingBox> intersectedBoxes = cellGrid.getAllInersectedBoundingBoxes(t.getBoundingBox());
			
			for(final BoundingBox box : intersectedBoxes) {
				if(bboxes.containsKey(box)) {
					final int oldValue = bboxes.get(box);
					bboxes.put(box, oldValue + 1);
				} else {
					bboxes.put(box, 1);
				}
			}
		});
		
		try {
			System.out.println("# Processing tuples");
			tupleFile.processFile();
		} catch (IOException e) {
			System.err.println("Got an IOException during experiment: "+ e);
			System.exit(-1);
		}
		
		calculateResult(bboxes);
	}
	
	/**
	 *  Calculate the result
	 */
	protected void calculateResult(final Map<BoundingBox, Integer> bboxes) {
		System.out.println("# Calculating node results");
		
		final int[] boxesPerNode = new int[NODES];
		for(int i = 0; i < boxesPerNode.length; i++) {
			boxesPerNode[i] = 0;
		}
		
		for(final BoundingBox key : bboxes.keySet()) {
			final int value = bboxes.get(key);
			boxesPerNode[key.hashCode() % NODES] += value; 
		}
		
		System.out.println("#Node\tValues");
		for(int i = 0; i < boxesPerNode.length; i++) {
			System.out.format("%d\t%d\n", i, boxesPerNode[i]);
		}
	}

	/**
	 * Determine global bounding box
	 * @param sampleSize
	 * @return 
	 * @throws IOException 
	 */
	protected BoundingBox determineBoundingBox() {
		System.out.println("# Determining the bounding box");
			
		final TupleFileReader tupleFile = new TupleFileReader(filename, format);
		final List<BoundingBox> bboxes = new ArrayList<>();
		
		tupleFile.addTupleListener(t -> {
			bboxes.add(t.getBoundingBox());
		});
		
		try {
			tupleFile.processFile();
		} catch (IOException e) {
			System.err.println("Got an IOException during experiment: "+ e);
			System.exit(-1);
		}
		
		return BoundingBox.getCoveringBox(bboxes);
	}
	
	/**
	 * Main * Main * Main
	 */
	public static void main(final String[] args) throws IOException {
		
		// Check parameter
		if(args.length != 3) {
			System.err.println("Usage: programm <filename> <format> <cells per dimension>");
			System.exit(-1);
		}
		
		final String filename = Objects.requireNonNull(args[0]);
		final String format = Objects.requireNonNull(args[1]);
		final double cells = MathUtil.tryParseDoubleOrExit(args[2]);
		
		final TestFixedGrid testSplit = new TestFixedGrid(filename, format, cells);
		testSplit.run();
	}
}
