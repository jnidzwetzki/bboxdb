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
package org.bboxdb.experiments.conference;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.concurrent.ExecutorUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.experiments.ExperimentHelper;
import org.bboxdb.storage.entity.CellGrid;
import org.bboxdb.tools.TupleFileReader;

public class TestFixedGrid implements Runnable {

	/**
	 * The cell sizes
	 */
	private final List<Integer> cellSizes;

	/**
	 * The files to process
	 */
	private final Map<String, String> filesAndFormats;

	/**
	 * The number of storing nodes
	 */
	public final static int NODES = 385;


	public TestFixedGrid(final Map<String, String> filesAndFormats, final List<Integer> cellSizes) {
		this.filesAndFormats = filesAndFormats;
		this.cellSizes = cellSizes;
	}

	@Override
	public void run() {
		final Hyperrectangle boundingBox = filesAndFormats
				.entrySet()
				.stream()
				.map(e -> ExperimentHelper.determineBoundingBox(e.getKey(), e.getValue()))
				.reduce((b1, b2) -> Hyperrectangle.getCoveringBox(b1, b2))
				.orElseThrow(() -> new IllegalArgumentException("Unable to calculate bounding box"));

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
	private void runExperiment(final CellGrid cellGrid) {

		final Map<Hyperrectangle, Integer> bboxes = new HashMap<>();

		for(Entry<String, String> file : filesAndFormats.entrySet()) {

			final TupleFileReader tupleFile = new TupleFileReader(file.getKey(), file.getValue());

			final ExecutorService executor = ExecutorUtil.getBoundThreadPoolExecutor(10, 100);

			tupleFile.addTupleListener(t -> {
				executor.submit(() -> {
					final Set<Hyperrectangle> intersectedBoxes = cellGrid.getAllInersectedBoundingBoxes(t.getBoundingBox());

					synchronized (bboxes) {
						for(final Hyperrectangle box : intersectedBoxes) {
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
				System.out.println("# Processing tuples in file: " + file.getKey());
				tupleFile.processFile();
				executor.shutdown();
			} catch (Exception e) {
				System.err.println("Got an Exception during experiment: " + e);
				System.exit(-1);
			}
		}

		calculateResult(bboxes);
	}

	/**
	 *  Calculate the result
	 */
	private void calculateResult(final Map<Hyperrectangle, Integer> bboxes) {
		System.out.println("# Calculating node results");

		final int[] boxesPerNode = new int[NODES];
		for(int i = 0; i < boxesPerNode.length; i++) {
			boxesPerNode[i] = 0;
		}

		for(final Entry<Hyperrectangle, Integer> entry : bboxes.entrySet()) {
			final int pos = Math.abs(entry.getKey().hashCode() % NODES);
			boxesPerNode[pos] += entry.getValue();
		}

		System.out.println("#Node\tValues");
		for(int i = 0; i < boxesPerNode.length; i++) {
			System.out.format("%d\t%d%n", i, boxesPerNode[i]);
		}
	}

	/**
	 * Main * Main * Main
	 */
	public static void main(final String[] args) throws IOException {

		// Check parameter
		if(args.length < 2) {
			System.err.println("Usage: programm <cells per dimension> <filename1:format1> <filenameN:formatN> ");
			System.exit(-1);
		}

		final List<Integer> cellSizes = Arrays.asList(args[0].split(","))
				.stream()
				.map(s -> MathUtil.tryParseIntOrExit(s, () -> "Unable to parse: " + s))
				.collect(Collectors.toList());


		final Map<String, String> filesAndFormats = new HashMap<>();

		for(int pos = 1; pos < args.length; pos++) {

			final String element = args[pos];

			if(! element.contains(":")) {
				System.err.println("Element does not contain format specifier: " + element);
				System.exit(-1);
			}

			final String[] splitFile = element.split(":");

			if(splitFile.length != 2) {
				System.err.println("Unable to get two elements after format split: " + element);
				System.exit(-1);
			}

			filesAndFormats.put(splitFile[0], splitFile[1]);
		}

		final TestFixedGrid testSplit = new TestFixedGrid(filesAndFormats, cellSizes);
		testSplit.run();
	}
}
