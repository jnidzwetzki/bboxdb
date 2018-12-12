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
package org.bboxdb.networkproxy.misc;

import org.bboxdb.storage.entity.Tuple;

public class TupleSerializer {
	
	/**
	 * Convert the tuple into a string
	 * @param tuple
	 * @param sb
	 * @return
	 */
	public static String tupleToProxyString(final Tuple tuple) {
		final StringBuilder sb = new StringBuilder();
		
		sb.append(tuple.getKey().length());
		sb.append(" ");
		sb.append(tuple.getKey());
		sb.append(" ");
		sb.append(tuple.getBoundingBox().toCompactString());
		sb.append(" ");
		sb.append(tuple.getDataBytes().length);
		sb.append(" ");
		sb.append(tuple.getDataBytes());
		sb.append(" ");
		sb.append(tuple.getVersionTimestamp());
		
		return sb.toString();
	}
	
	/**
	 * Convert the proxy string back to a tuple
	 * @param string
	 * @return
	 */
	public static Tuple proxyStringToTuple(final String string) {
		return new Tuple(null, null, null);
	}
}
