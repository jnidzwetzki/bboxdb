/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.storage;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public class BloomFilterBuilder {

	/**
	 * Create a bloom filter for a given number of keys
	 * @param entries
	 * @return
	 */
	public static BloomFilter<String> buildBloomFilter(final long entries) {
		return BloomFilter.create(new Funnel<String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -2545258829029691131L;

			@Override
			public void funnel(final String argument, final PrimitiveSink into) {
				into.putString(argument, Charsets.UTF_8);
			}
		}, entries);
	}
}
