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

public class JDBCH2TupleStore extends AbstractJDBCTupleStore {
	
    /**	
     * The H2 DB file flags
     */
    protected final static String DB_FLAGS = ";CACHE_SIZE=131072";
    
	public JDBCH2TupleStore(final File dir) throws Exception {
		super(dir);
	}

	@Override
	protected String getConnectionURL() {
		return "jdbc:h2:nio:" + getDBFile().getAbsolutePath() + DB_FLAGS;
	}

	@Override
	public String getCreateTableSQL() {
		return "CREATE TABLE IF NOT EXISTS tuples (id INT PRIMARY KEY, data BLOB)";
	}

	@Override
	public File getDBFile() {
		return new File(dir.getAbsolutePath() + "/dbtest.db");
	}
}
