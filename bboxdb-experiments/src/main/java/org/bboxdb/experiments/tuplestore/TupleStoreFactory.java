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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TupleStoreFactory {
	
	/**
	 * The BDB tuple store
	 */
	public final static String TUPLE_STORE_BDB = "bdb";
	
	/**
	 * The JDBC h2 tuple store
	 */
	public final static String TUPLE_STORE_JDBC_H2 = "jdbc-h2";

	/**
	 * The JDBC derby tuple store
	 */
	public final static String TUPLE_STORE_JDBC_DERBY = "jdbc-derby";

	/**
	 * The SSTable tuple store
	 */
	public final static String TUPLE_STORE_SSTABLE = "sstable";
	
	/**
	 * All tuple stores
	 */
	public final static List<String> ALL_STORES = Collections.unmodifiableList(
			Arrays.asList(TUPLE_STORE_BDB, TUPLE_STORE_JDBC_H2, TUPLE_STORE_JDBC_DERBY,
					TUPLE_STORE_SSTABLE));
	
	/**
	 * Return the tuple store for the name
	 * @param tupleStoreName
	 * @return
	 * @throws IOException 
	 */
	public static TupleStore getTupleStore(final String tupleStoreName, final File dir) throws Exception {
		switch(tupleStoreName) {
		case TUPLE_STORE_BDB:
			return new BDBTupleStore(dir);
		case TUPLE_STORE_JDBC_H2:
			return new JDBCH2TupleStore(dir);
		case TUPLE_STORE_JDBC_DERBY:
			return new JDBCDerbyTupleStore(dir);
		case TUPLE_STORE_SSTABLE: 
			return new SSTableTupleStore(dir);
		default:
			throw new IllegalArgumentException("Unknown tuple store: " + tupleStoreName);
		}
	}

}
