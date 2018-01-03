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
package org.bboxdb.storage;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public class BloomFilterBuilder {

	private static final class TupleKeyFunnel implements Funnel<String> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2545258829029691131L;

		@Override
		public void funnel(final String argument, final PrimitiveSink into) {
			into.putString(argument, Charsets.UTF_8);
		}
	}

	/**
	 * Create a bloom filter for a given number of keys
	 * @param entries
	 * @return
	 */
	public static BloomFilter<String> buildBloomFilter(final long entries) {
		return BloomFilter.create(new TupleKeyFunnel(), entries);
	}
	
	/**
	 * Load a persistent bloom filter into memory
	 * @param file
	 * @return 
	 * @throws IOException
	 */
	public static BloomFilter<String> loadBloomFilterFromFile(final File file) throws IOException {
		
		try(final InputStream inputStream = new BufferedInputStream(new FileInputStream(file));) {
			return BloomFilter.readFrom(inputStream, new TupleKeyFunnel());
		} catch(IOException e) {
			throw e;
		}
	}
}
