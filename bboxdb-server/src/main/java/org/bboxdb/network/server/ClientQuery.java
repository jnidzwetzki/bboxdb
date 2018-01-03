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
package org.bboxdb.network.server;

import java.io.Closeable;
import java.io.IOException;

import org.bboxdb.network.packages.PackageEncodeException;

public interface ClientQuery extends Closeable {

	/**
	 * Calculate the next tuples of the query
	 * @param packageSequence2 
	 * @return
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	public void fetchAndSendNextTuples(short packageSequence) throws IOException, PackageEncodeException;

	/**
	 * Is the current query done
	 * @return
	 */
	public boolean isQueryDone();
	
	/**
	 * Close the client query
	 */
	public void close();

	/**
	 * Get the amount of total send tuples
	 * @return
	 */
	public long getTotalSendTuples();

}