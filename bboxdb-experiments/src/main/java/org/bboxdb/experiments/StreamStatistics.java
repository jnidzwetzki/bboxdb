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
package org.bboxdb.experiments;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.bboxdb.tools.TupleFileReader;

public class StreamStatistics {

	/***
	 * Main * Main * Main * Main
	 * @param args
	 */
	public static void main(final String[] args) {
		
		if(args.length != 2) {
			System.err.println("Usage: <Class> <Filename> <Format>");
			System.exit(-1);
		}
		
		final String filename = args[0];
		final String inputformat = args[1];
		
		final File file = new File(filename);
		
		if(! file.isFile()) {
			System.err.println("Unable to open: " + file);
			System.exit(-1);
		}
		
		System.out.println("Processing file: " + filename + " with format " + inputformat);
		
		
		final TupleFileReader tupleFile = new TupleFileReader(filename, inputformat);
		final AtomicLong processedTuples = new AtomicLong(0);
		final AtomicLong fiveSecBucket = new AtomicLong(0);
		final AtomicLong lastBucketBegin = new AtomicLong(-1);
		final AtomicLong maxBurst = new AtomicLong(0);
		
		tupleFile.addTupleListener(t -> {
			
			if(lastBucketBegin.get() < 0) {
				lastBucketBegin.set(t.getVersionTimestamp());
			}
			
			// Create a five sec bucket and count max throughput
			if(lastBucketBegin.get() + 5000 > t.getVersionTimestamp()) {
				fiveSecBucket.incrementAndGet();
			} else {
				if(maxBurst.get() < fiveSecBucket.get()) {
					maxBurst.set(fiveSecBucket.get());
				}
				// Start new Bucket
				fiveSecBucket.set(0);
				lastBucketBegin.set(t.getVersionTimestamp());
			}
			
			processedTuples.incrementAndGet();
		});

		try {
			tupleFile.processFile();
			System.out.println("Processed " + processedTuples.get() + " elements");
			System.out.println("Max element throughut per sec: " + maxBurst.get() / 5);
		} catch (Exception e) {
			System.err.println("Got an Exception during experiment: "+ e);
			System.exit(-1);
		}
		
	}
}
