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

public class JDBCDerbyTupleStore extends AbstractJDBCTupleStore {

	public JDBCDerbyTupleStore(final File dir) throws Exception {
		super(dir);
	}

	@Override
	protected String getConnectionURL() {
		return "jdbc:derby:" + getDBFile().getAbsolutePath() + ";create=true";
	}

	@Override
	public String getCreateTableSQL() {
		return "CREATE TABLE tuples (id INT, data BLOB, PRIMARY KEY (id))";
	}

	@Override
	public File getDBFile() {
		return new File(dir.getAbsolutePath());
	}
}
