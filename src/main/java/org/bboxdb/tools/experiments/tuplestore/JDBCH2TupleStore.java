/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.tools.experiments.tuplestore;

import java.io.File;
import java.sql.SQLException;

public class JDBCH2TupleStore extends AbstractJDBCTupleStore {
	
    /**
     * The H2 DB file flags
     */
    protected final static String DB_FLAGS = ";LOG=0;CACHE_SIZE=262144;LOCK_MODE=0;UNDO_LOG=0";
    
	public JDBCH2TupleStore(final File dir) throws SQLException {
		super(dir);
	}

	@Override
	protected String getConnectionURL() {
		return "jdbc:h2:nio:" + dir.getAbsolutePath() + "/dbtest.db" + DB_FLAGS;
	}
}
