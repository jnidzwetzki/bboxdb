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
package org.bboxdb.experiments.tuplestore;

import java.io.IOException;

import org.bboxdb.storage.entity.Tuple;

public interface TupleStore extends AutoCloseable {

	/**
	 * Store the given tuple
	 * @param tuple
	 * @throws IOException 
	 */
	public void writeTuple(final Tuple tuple) throws Exception;
	
	/**
	 * Read the given tuple
	 * @param key
	 * @throws IOException
	 * @return
	 */
	public Tuple readTuple(final String key) throws Exception;
	
	/**
	 * Open the given tuple store
	 * @throws Exception
	 */
	public void open() throws Exception;

}
