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
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.GeoJsonPolygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyzeAuData implements Runnable {
	
	/**
	 * The file to read
	 */
	private final File file;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(AnalyzeAuData.class);
	
	
	public AnalyzeAuData(final File file) {
		this.file = file;
	}

	@Override
	public void run() {
		
		try (
				final BufferedReader reader = new BufferedReader(new FileReader(file));
		) {
			String line = null;
			String currentTimeslot = null;
			long readElementInTimeframe = 0;
			final SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh");
			
			while((line = reader.readLine()) != null) {
				final GeoJsonPolygon polygon = GeoJsonPolygon.fromGeoJson(line);
				
				final String timestampString = polygon.getProperties().get("Timestamp");
				final long timestamp = MathUtil.tryParseLong(timestampString, 
						() -> "Unable to parse: " + timestampString);
				
				final String timeslot = sdf.format(new Date(timestamp));
				
				if(currentTimeslot == null) {
					currentTimeslot = timeslot;
					continue;
				}
				
				if(! currentTimeslot.equals(timeslot)) {
					System.out.println(currentTimeslot + "\t" + readElementInTimeframe);
					readElementInTimeframe = 1;
					currentTimeslot = timeslot;
					continue;
				}
				
				readElementInTimeframe++;
			}
			
		} catch (Exception e) {
			logger.error("Got error", e);
		} 
	}

	/**
	 * Main Main Main Main
	 * @param args
	 */
	public static void main(final String[] args) {
		
		if(args.length != 1) {
			System.err.println("Usage: <Class> <Filename>");
			System.exit(-1);
		}
		
		final File file = new File(args[0]);
		
		if(! file.isFile()) {
			System.err.println("Unable to open: " + file);
			System.exit(-1);
		}
		
		final AnalyzeAuData analyzeAuData = new AnalyzeAuData(file);
		analyzeAuData.run();
	}
}