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
package org.bboxdb.tools.network;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.converter.tuple.TupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

public class ImportStoredDataTimed implements Runnable {
	
	/**
	 * The file to read
	 */
	private final File file;
	
	/**
	 * The destination socket
	 */
	private final Socket socket;

	/**
	 * The number of lines to import
	 */
	private TupleBuilder tupleFactory;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ImportStoredDataTimed.class);
	
	public ImportStoredDataTimed(final File file, final Socket socket, final TupleBuilder tupleFactory) {
		this.file = file;
		this.socket = socket;
		this.tupleFactory = tupleFactory;
	}

	@Override
	public void run() {
		
		try (
				final BufferedReader reader = new BufferedReader(new FileReader(file));
				final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
		) {
			String line = null;
			long currentSecondSlot = getTimeSlot();
			long totalProcessedLines = 0;
			long processedLines = 0;
			final Stopwatch stopwatch = Stopwatch.createStarted();
			long timeOffset = -1;
			
			while((line = reader.readLine()) != null) {
				writer.write(line + "\n");
				totalProcessedLines++;
				processedLines++;
				
				if(Thread.currentThread().isInterrupted()) {
					logger.info("Thread is interruped");
					return;
				}

				final Tuple tuple = tupleFactory.buildTuple(line);
				
				// Not all lines (like adsb messages generate a tuple)
				if(tuple == null) {
					continue;
				}
				
				
				if(timeOffset == -1) {
					timeOffset = (System.currentTimeMillis() * 1000) - tuple.getVersionTimestamp();
				}
				
				// System.out.println("Tuple timestamp: " + tuple.getVersionTimestamp());
				// System.out.println("Current time: " + System.currentTimeMillis() * 1000);
				
				while(tuple.getVersionTimestamp() + timeOffset > System.currentTimeMillis() * 1000) {

					if(Thread.currentThread().isInterrupted()) {
						logger.info("Thread is interruped");
						return;
					}
					
					Thread.sleep(10);
				}
			
				// Dump time slot statistics
				final long curTimeSlot = getTimeSlot();
				
				if(curTimeSlot > currentSecondSlot) {
					logger.info("Processed {} elements", processedLines);
					processedLines = 0;
					currentSecondSlot = curTimeSlot;
					writer.flush();
					continue;
				}				
			}
			
			logger.info("Total processed elements {} in {} ms", 
					totalProcessedLines, stopwatch.elapsed(TimeUnit.MILLISECONDS));
		} catch (Exception e) {
			logger.error("Got error", e);
		} 
	}

	/**
	 * Get the time slot
	 * @return
	 */
	private long getTimeSlot() {
		return System.currentTimeMillis() / 1000;
	}

	/**
	 * Main Main Main Main
	 * @param args
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */
	public static void main(final String[] args) throws UnknownHostException, IOException {
		
		if(args.length != 3) {
			System.err.println("Usage: <Class> <Filename> <Host:Port> <Data format>");
			System.exit(-1);
		}
		
		final File file = new File(args[0]);
		final String[] networkSocketString = args[1].split(":");
		final TupleBuilder tupleFactory = TupleBuilderFactory.getBuilderForFormat(args[2]);
		
		if(! file.isFile()) {
			System.err.println("Unable to open: " + file);
			System.exit(-1);
		}
		
		final int port = MathUtil.tryParseIntOrExit(networkSocketString[1], 
				() -> "Unable to parse: " + networkSocketString[1]);
		
		final Socket socket = new Socket(networkSocketString[0], port);
		
		final ImportStoredDataTimed analyzeAuData = new ImportStoredDataTimed(file, socket, tupleFactory);
		analyzeAuData.run();
	}
}