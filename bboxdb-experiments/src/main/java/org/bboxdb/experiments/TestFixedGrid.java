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
package org.bboxdb.experiments;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.concurrent.ExecutorUtil;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.storage.entity.CellGrid;
import org.bboxdb.tools.TupleFileReader;

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
	 * The cell sizes
	 */
	protected List<Integer> cellSizes;
	
	/**
	 * The number of storing nodes
	 */
	public final static int NODES = 385;
	
	public TestFixedGrid(final String filename, final String format, final List<Integer> cellSizes) {
		this.filename = filename;
		this.format = format;
		this.cellSizes = cellSizes;
	}
	
	@Override
	public void run() {
		System.out.format("Reading %s\n", filename);
		final BoundingBox boundingBox = ExperimentHelper.determineBoundingBox(filename, format);
		
		for(final Integer cellsPerDimension: cellSizes) {
			System.out.println("Cells per Dimension: " + cellsPerDimension);
			final CellGrid cellGrid = CellGrid.buildWithFixedAmountOfCells(boundingBox, cellsPerDimension);
			runExperiment(cellGrid);
		}
	}

	/**
	 * Run this experiment
	 * @param cellGrid 
	 */
	protected void runExperiment(final CellGrid cellGrid) {
		final TupleFileReader tupleFile = new TupleFileReader(filename, format);
		final Map<BoundingBox, Integer> bboxes = new HashMap<>();
		
		final ExecutorService executor = ExecutorUtil.getBoundThreadPoolExecutor(10, 100);
		
		tupleFile.addTupleListener(t -> {
			executor.submit(() -> {
				final Set<BoundingBox> intersectedBoxes = cellGrid.getAllInersectedBoundingBoxes(t.getBoundingBox());
				
				synchronized (bboxes) {
					for(final BoundingBox box : intersectedBoxes) {
						if(bboxes.containsKey(box)) {
							final int oldValue = bboxes.get(box);
							bboxes.put(box, oldValue + 1);
						} else {
							bboxes.put(box, 1);
						}
					}	
				}
			});
		});

		try {
			System.out.println("# Processing tuples");
			tupleFile.processFile();
			executor.shutdown();
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
			int pos = Math.abs(key.hashCode() % NODES);
			boxesPerNode[pos] += value; 
		}
		
		System.out.println("#Node\tValues");
		for(int i = 0; i < boxesPerNode.length; i++) {
			System.out.format("%d\t%d\n", i, boxesPerNode[i]);
		}
	}
	
	/**
	 * Main * Main * Main
	 */
	public static void main(final String[] args) throws IOException {
		
		// Check parameter
		if(args.length < 3) {
			System.err.println("Usage: programm <filename> <format> <cells per dimension>");
			System.exit(-1);
		}
		
		final String filename = Objects.requireNonNull(args[0]);
		final String format = Objects.requireNonNull(args[1]);
		
		final List<Integer> cellSizes = new ArrayList<>();
		
		for(int pos = 2; pos < args.length; pos++) {
			final Integer size = MathUtil.tryParseIntOrExit(args[pos]);
			cellSizes.add(size);
		}
				
		final TestFixedGrid testSplit = new TestFixedGrid(filename, format, cellSizes);
		testSplit.run();
	}
}
