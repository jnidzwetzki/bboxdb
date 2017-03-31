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
package org.bboxdb.performance.experiments;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.CompressionEnvelopeRequest;
import org.bboxdb.network.packages.request.InsertTupleRequest;
import org.bboxdb.performance.osm.OSMFileReader;
import org.bboxdb.performance.osm.OSMStructureCallback;
import org.bboxdb.performance.osm.OSMType;
import org.bboxdb.performance.osm.util.Polygon;
import org.bboxdb.performance.osm.util.SerializerHelper;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCompressionRatio implements Runnable, OSMStructureCallback {

	/**
	 * The file to read
	 */
	protected String filename;
	
	/**
	 * The type to import
	 */
	protected OSMType osmType;
	
	/**
	 * The db instance
	 */
	protected final DB db;
	
	/**
	 * The node map
	 */
	protected final Map<Long, byte[]> nodeMap;
	
	/**
	 * The element counter
	 */
	protected long elementCounter;
	
	/**
	 * The node serializer
	 */
	protected final SerializerHelper<Polygon> serializerHelper = new SerializerHelper<>();

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TestCompressionRatio.class);
	

	public TestCompressionRatio(final String filename, final OSMType osmType) throws IOException {
		this.filename = filename;
		this.osmType = osmType;
		
		final File dbFile = File.createTempFile("osm-db-sampling", ".tmp");
		dbFile.delete();
		
		// Use a disk backed map, to process files > Memory
		this.db = DBMaker.fileDB(dbFile).fileMmapEnableIfSupported().fileDeleteAfterClose().make();
		this.nodeMap = db.hashMap("osm-id-map").keySerializer(Serializer.LONG)
		        .valueSerializer(Serializer.BYTE_ARRAY)
		        .create();
		
		elementCounter = 0;
	}

	@Override
	public void run() {
		long baseSize = -1;
		
		System.out.format("Importing %s\n", filename);
		final OSMFileReader osmFileReader = new OSMFileReader(filename, osmType, this);
		osmFileReader.run();
		final int numberOfElements = nodeMap.keySet().size();
		System.out.format("Imported %d objects\n", numberOfElements);
		
		if(numberOfElements == 0) {
			System.err.println("No data is imported, stopping run");
			System.exit(-1);
		}
		
		final List<Integer> bachSizes = Arrays.asList(
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
				20, 30, 40, 50, 60, 70, 80, 90, 100);
		
		System.out.println("Bachsize\tDatasize\tRatio\tCompression ratio");
		
		for(final Integer batchSize : bachSizes) {
			try {
				
				final long experimentSize = runExperiment(batchSize);
				
				if(batchSize == 0 && baseSize == -1) {
					baseSize = experimentSize;
				}
				
				float diff = baseSize - experimentSize;
				final float pDiff = diff / (float) baseSize * 100;
				
				final double ratio = (float) experimentSize / (float) baseSize * 100.0;
				System.out.format("%d\t%d\t%f\t%f\n", batchSize, experimentSize, ratio, pDiff);

			} catch (ClassNotFoundException | IOException | PackageEncodeException e) {
				logger.error("Exception while running experiment", e);
			}
		}
	}


	/**
	 * Run the experiment with the given batch size
	 * @param batchSize
	 * @return 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws PackageEncodeException 
	 */
	protected long runExperiment(final Integer batchSize) throws ClassNotFoundException, IOException, PackageEncodeException {
		final SSTableName tableName = new SSTableName("2_group1_table1");
		long totalSize = 0;
		
		final List<Tuple> buffer = new ArrayList<>();

		for(final Long id : nodeMap.keySet()) {
			final byte[] elementBytes = nodeMap.get(id);
			final Polygon element = serializerHelper.loadFromByteArray(elementBytes);
			final Tuple tuple = new Tuple(Long.toString(id), element.getBoundingBox(), element.toGeoJson().getBytes());

			if(batchSize == 0) {
				final int size = handleUncompressedData(tableName, tuple);
				totalSize = totalSize + size;
			} else if (buffer.size() == batchSize) {
				final int size = handleCompressedData(tableName, buffer);
				totalSize = totalSize + size;
			} else {
				buffer.add(tuple);
			}
		}

		return totalSize;
	}

	/**
	 * Handle compressed packages
	 * @param tableName
	 * @param buffer
	 * @return
	 * @throws PackageEncodeException
	 * @throws IOException
	 */
	protected int handleCompressedData(final SSTableName tableName, final List<Tuple> buffer)
			throws PackageEncodeException, IOException {
		
		final List<NetworkRequestPackage> packages = 
				buffer
				.stream()
				.map(t -> new InsertTupleRequest((short) 4, tableName, t))
				.collect(Collectors.toList());
				
		final CompressionEnvelopeRequest compressionEnvelopeRequest 
			= new CompressionEnvelopeRequest(NetworkConst.COMPRESSION_TYPE_GZIP, packages);
		
		buffer.clear();
		
		final int size = packageToBytes(compressionEnvelopeRequest);
		return size;
	}

	/**
	 * Handle the uncompressed version
	 * @param tableName 
	 * @return
	 * @throws PackageEncodeException
	 * @throws IOException
	 */
	protected int handleUncompressedData(final SSTableName tableName, final Tuple tuple)
			throws PackageEncodeException, IOException {
		
		final InsertTupleRequest insertTupleRequest = new InsertTupleRequest((short) 4, tableName, tuple);
		return packageToBytes(insertTupleRequest);
	}

	/**
	 * Convert the given package into a byte stream 
	 * @param networkPackage
	 * @return
	 * @throws PackageEncodeException
	 * @throws IOException
	 */
	protected int packageToBytes(final NetworkRequestPackage networkPackage)
			throws PackageEncodeException, IOException {
		
		final ByteArrayOutputStream os = new ByteArrayOutputStream();
		networkPackage.writeToOutputStream(os);
		os.close();
		return os.toByteArray().length;
	}

	public static void main(final String[] args) throws IOException {
		
		// Check parameter
		if(args.length != 2) {
			System.err.println("Usage: programm <filename> <" + OSMFileReader.getFilterNames() + ">");
			System.exit(-1);
		}
		
		final String filename = args[0];
		final String type = args[1];
		
		// Check file
		final File inputFile = new File(filename);
		if(! inputFile.isFile()) {
			System.err.println("Unable to open file: " + filename);
			System.exit(-1);
		}
		
		// Check type
		final OSMType osmType = OSMType.fromString(type);
		if(osmType == null) {
			System.err.println("Unknown type: " + type);
			System.exit(-1);
		}
		
		final TestCompressionRatio testCompressionRatio = new TestCompressionRatio(filename, osmType);
		testCompressionRatio.run();
	}

	@Override
	public void processStructure(Polygon geometricalStructure) {
		try {
			final byte[] data = serializerHelper.toByteArray(geometricalStructure);
			nodeMap.put(elementCounter++, data);
		} catch (IOException e) {
			logger.error("Got an exception during encoding", e);
		}
	}
}
