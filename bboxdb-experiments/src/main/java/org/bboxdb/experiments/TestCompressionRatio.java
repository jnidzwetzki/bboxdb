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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.CompressionEnvelopeRequest;
import org.bboxdb.network.packages.request.InsertTupleRequest;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.tools.TupleFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCompressionRatio implements Runnable {

	/**
	 * The file to read
	 */
	protected String filename;
	
	/**
	 * The format of the input file
	 */
	protected String format;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TestCompressionRatio.class);	

	public TestCompressionRatio(final String filename, final String format) throws IOException {
		this.filename = filename;
		this.format = format;
	}

	@Override
	public void run() {
		long baseSize = -1;
		
		System.out.format("Reading %s\n", filename);
		
		final List<Integer> bachSizes = Arrays.asList(
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
				20, 30, 40, 50, 60, 70, 80, 90, 100);
		
		System.out.println("# Batchsize\tDatasize\tRatio\tCompression ratio");
		
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
		final TupleStoreName tableName = new TupleStoreName("2_group1_table1");
		final List<Long> experimentSize = new ArrayList<>();
		
		final List<Tuple> buffer = new ArrayList<>();
		final TupleFileReader tupleFile = new TupleFileReader(filename, format);
		
		tupleFile.addTupleListener(t -> {
			if(batchSize == 0) {
				final long size = handleUncompressedData(tableName, t);
				experimentSize.add(size);
			} else if (buffer.size() == batchSize) {
				final long size = handleCompressedData(tableName, buffer);
				experimentSize.add(size);
			} else {
				buffer.add(t);
			}
		});
	
		try {
			tupleFile.processFile();
		} catch (IOException e) {
			logger.error("Got an IO-Exception while reading file", e);
			System.exit(-1);
		}

		return experimentSize.stream().mapToLong(i -> i).sum();
	}

	/**
	 * Handle compressed packages
	 * @param tableName
	 * @param buffer
	 * @return
	 * @throws PackageEncodeException
	 * @throws IOException
	 */
	protected long handleCompressedData(final TupleStoreName tableName, final List<Tuple> buffer) {
		
		final Supplier<RoutingHeader> routingHeaderSupplier = () -> (new RoutingHeader(false));
		
		final List<NetworkRequestPackage> packages = 
				buffer
				.stream()
				.map(t -> new InsertTupleRequest((short) 4, routingHeaderSupplier, tableName, t))
				.collect(Collectors.toList());
				
		final CompressionEnvelopeRequest compressionEnvelopeRequest 
			= new CompressionEnvelopeRequest(NetworkConst.COMPRESSION_TYPE_GZIP, packages);
		
		buffer.clear();
		
		return packageToBytes(compressionEnvelopeRequest);
	}

	/**
	 * Handle the uncompressed version
	 * @param tableName 
	 * @return
	 */
	protected long handleUncompressedData(final TupleStoreName tableName, final Tuple tuple) {
	
		final Supplier<RoutingHeader> routingHeaderSupplier = () -> (new RoutingHeader(false));

		final InsertTupleRequest insertTupleRequest = new InsertTupleRequest(
				(short) 4, 
				routingHeaderSupplier, 
				tableName, 
				tuple);
		
		return packageToBytes(insertTupleRequest);
	}

	/**
	 * Convert the given package into a byte stream 
	 * @param networkPackage
	 * @return
	 */
	protected long packageToBytes(final NetworkRequestPackage networkPackage) {
		
		try {
			final ByteArrayOutputStream os = new ByteArrayOutputStream();
			networkPackage.writeToOutputStream(os);
			os.close();
			return os.toByteArray().length;
		} catch (IOException e) {
			logger.error("Got an IO-Exception while closing stream", e);
			System.exit(-1);
		} catch (PackageEncodeException e) {
			logger.error("Got an Package encode exception", e);
			System.exit(-1);
		}
		
		// Unreachable code
		return -1;
	}

	/**
	 * Main * Main * Main * Main
	 * @param args
	 */
	public static void main(final String[] args) throws IOException {
		
		// Check parameter
		if(args.length != 2) {
			System.err.println("Usage: programm <filename> <format>");
			System.exit(-1);
		}
		
		final String filename = Objects.requireNonNull(args[0]);
		final String format = Objects.requireNonNull(args[1]);
		
		final TestCompressionRatio testCompressionRatio = new TestCompressionRatio(filename, format);
		testCompressionRatio.run();
	}

}
