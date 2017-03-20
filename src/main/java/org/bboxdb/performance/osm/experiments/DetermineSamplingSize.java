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
package org.bboxdb.performance.osm.experiments;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.bboxdb.performance.osm.OSMFileReader;
import org.bboxdb.performance.osm.OSMStructureCallback;
import org.bboxdb.performance.osm.util.Polygon;
import org.bboxdb.performance.osm.util.SerializerHelper;
import org.bboxdb.storage.entity.BoundingBox;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DetermineSamplingSize implements Runnable, OSMStructureCallback {

	/**
	 * The db instance
	 */
	protected final DB db;
	
	/**
	 * The node map
	 */
	protected final Map<Long, byte[]> nodeMap;
	
	/**
	 * The node serializer
	 */
	protected final SerializerHelper<Polygon> serializerHelper = new SerializerHelper<>();

	/**
	 * The file to import
	 */
	protected final String filename;
	
	/**
	 * The element counter
	 */
	protected long elementCounter;

	/**
	 * The retry counter for the experiments
	 */
	public final static int EXPERIMENT_RETRY = 10;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DetermineSamplingSize.class);
	
	public DetermineSamplingSize(final String filename) throws IOException {
		this.filename = filename;
		
		this.elementCounter = 0;
		
		final File dbFile = File.createTempFile("osm-db-sampling", ".tmp");
		dbFile.delete();
		
		// Use a disk backed map, to process files > Memory
		this.db = DBMaker.fileDB(dbFile).fileMmapEnableIfSupported().fileDeleteAfterClose().make();
		this.nodeMap = db.hashMap("osm-id-map").keySerializer(Serializer.LONG)
		        .valueSerializer(Serializer.BYTE_ARRAY)
		        .create();
	}
	
	@Override
	public void run() {
		System.out.format("Importing %s\n", filename);
		final OSMFileReader osmFileReader = new OSMFileReader(filename, "tree", this);
		osmFileReader.run();
		final int numberOfElements = nodeMap.keySet().size();
		System.out.format("Imported %d objects\n", numberOfElements);
		
		final List<Double> sampleSizes = Arrays.asList(0.2d, 0.3d, 0.4d, 0.5d, 0.6d, 0.7d, 
				1d, 5d, 10d, 20d, 30d, 40d, 50d, 60d);
		
		for(final Double sampleSize : sampleSizes) {
			System.out.println("Simulating with sample size: " + sampleSize);
			final int numberOfSamples = (int) (numberOfElements / 100 * sampleSize);
			
			try {
				final double splitPos = getSplit(numberOfSamples);
				runExperimentForPos(splitPos);
			} catch (ClassNotFoundException | IOException e) {
				System.err.println(e.getStackTrace());
			}
		}
	}

	/**
	 * Run the experiment for the given position
	 * @param splitPos
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	protected void runExperimentForPos(final double splitPos) throws ClassNotFoundException, IOException {
		long left = 0;
		long right = 0;
		long total = 0;
	
		System.out.println("Got split position: " + splitPos);
		
		final BoundingBox fullBox = BoundingBox.createFullCoveringDimensionBoundingBox(2);
		final BoundingBox leftBox = fullBox.splitAndGetLeft(splitPos, 0, true);
		final BoundingBox rightBox = fullBox.splitAndGetRight(splitPos, 0, false);
		
		for(final Long id : nodeMap.keySet()) {
			final byte[] ploygonBytes = nodeMap.get(id);
			final Polygon polygon = serializerHelper.loadFromByteArray(ploygonBytes);
			
			final BoundingBox polygonBoundingBox = polygon.getBoundingBox();
			
			if(polygonBoundingBox.overlaps(leftBox)) {
				left++;
			}
			
			if(polygonBoundingBox.overlaps(rightBox)) {
				right++;
			}
			
			total++;
		}
		
		System.out.format("Total %d, left %d, right %d, diff %d\n", total, left, right, Math.abs(left - right));
	}
	
	/**
	 * Take a certain number of samples and generate a split position
	 * @param sampleSize
	 * @return 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	protected double getSplit(final float sampleSize) throws ClassNotFoundException, IOException {
		final Set<Long> takenSamples = new HashSet<>();
		final Random random = new Random(System.currentTimeMillis());
		final List<BoundingBox> samples = new ArrayList<>();

		while(takenSamples.size() < sampleSize) {
			final long sampleId = Math.abs(random.nextLong()) % elementCounter;
			
			if(takenSamples.contains(sampleId)) {
				continue;
			}
			
			takenSamples.add(sampleId);
			final byte[] ploygonBytes = nodeMap.get(sampleId);
			
			final Polygon polygon = serializerHelper.loadFromByteArray(ploygonBytes);
			samples.add(polygon.getBoundingBox());
		}
		
		samples.sort((b1, b2) -> Double.compare(b1.getCoordinateLow(0), b2.getCoordinateLow(0)));
		
		return samples.get(samples.size() / 2).getCoordinateLow(0);
	}

	@Override
	public void processStructure(final Polygon geometricalStructure) {
		try {
			final byte[] data = serializerHelper.toByteArray(geometricalStructure);
			nodeMap.put(elementCounter++, data);
		} catch (IOException e) {
			logger.error("Got an exception during encoding", e);
		}
	}
	
	/**
	 *
	 * Main * Main * Main
	 * @throws IOException 
	 * 
	 */
	public static void main(final String[] args) throws IOException {
		final String filename = "/Users/kristofnidzwetzki/Downloads/berlin-latest.osm.pbf";

		final DetermineSamplingSize determineSamplingSize = new DetermineSamplingSize(filename);
		determineSamplingSize.run();
	}

}
