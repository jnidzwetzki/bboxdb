/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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

import org.bboxdb.commons.MathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportStoredData implements Runnable {
	
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
	private final int linesPerSecond;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ImportStoredData.class);
	
	public ImportStoredData(final File file, final Socket socket, final int linesPerSecond) {
		this.file = file;
		this.socket = socket;
		this.linesPerSecond = linesPerSecond;
	}

	@Override
	public void run() {
		
		try (
				final BufferedReader reader = new BufferedReader(new FileReader(file));
				final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
		) {
			String line = null;
			long currentSecondSlot = System.currentTimeMillis() / 1000;
			int processedLines = 0;
			
			while((line = reader.readLine()) != null) {
				final long currentSecond = System.currentTimeMillis() / 1000;
				writer.write(line + "\n");
				processedLines++;
				
				if(currentSecond != currentSecondSlot) {
					currentSecondSlot = currentSecond;
					processedLines = 0;
					writer.flush();
					continue;
				}
				
				while((processedLines > linesPerSecond) && ! Thread.currentThread().isInterrupted()) {
					Thread.sleep(10);
				}
			}
		} catch (Exception e) {
			logger.error("Got error", e);
		} 
	}

	/**
	 * Main Main Main Main
	 * @param args
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */
	public static void main(final String[] args) throws UnknownHostException, IOException {
		
		if(args.length != 3) {
			System.err.println("Usage: <Class> <Filename> <Host:Port> <Elements per sec>");
			System.exit(-1);
		}
		
		final File file = new File(args[0]);
		final String[] networkSocketString = args[1].split(":");
		final int linesPerSecond = MathUtil.tryParseIntOrExit(args[2], () -> "Unable to parse: " + args[2]);

		if(! file.isFile()) {
			System.err.println("Unable to open: " + file);
			System.exit(-1);
		}
		
		final int port = MathUtil.tryParseIntOrExit(networkSocketString[1], 
				() -> "Unable to parse: " + networkSocketString[1]);
		
		final Socket socket = new Socket(networkSocketString[0], port);
		
		final ImportStoredData analyzeAuData = new ImportStoredData(file, socket, linesPerSecond);
		analyzeAuData.run();
	}
}