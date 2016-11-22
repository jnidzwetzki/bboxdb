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
package de.fernunihagen.dna.scalephant.storage.sstable;

import de.fernunihagen.dna.scalephant.storage.entity.DeletedTuple;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;

public class TupleHelper {

	/**
	 * Return the most recent version of the tuple
	 * @param tuple1
	 * @param tuple2
	 * @return
	 */
	public static Tuple returnMostRecentTuple(final Tuple tuple1, final Tuple tuple2) {
		if(tuple1 == null && tuple2 == null) {
			return null;
		}
		
		if(tuple1 == null) {
			return tuple2;
		}
		
		if(tuple2 == null) {
			return tuple1;
		}
		
		if(tuple1.getTimestamp() > tuple2.getTimestamp()) {
			return tuple1;
		}
		
		return tuple2;
	}

	/**
	 * If the tuple is a deleted tuple, return null
	 * Otherwise, return the given tuple
	 * @param tuple
	 * @return
	 */
	public static Tuple replaceDeletedTupleWithNull(final Tuple tuple) {
		
		if(tuple == null) {
			return null;
		}
		
		if(tuple instanceof DeletedTuple) {
			return null;
		}
		
		return tuple;
	}
}