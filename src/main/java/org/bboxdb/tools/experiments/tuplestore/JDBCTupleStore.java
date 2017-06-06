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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.TupleHelper;

public class JDBCTupleStore implements TupleStore {
	
    /**
     * The H2 DB file flags
     */
    protected final static String DB_FLAGS = ";LOG=0;CACHE_SIZE=262144;LOCK_MODE=0;UNDO_LOG=0";
    
    /**
     * The insert statement
     */
	private final PreparedStatement insertStatement;
	
	/**
	 * The select statement
	 */
	private final PreparedStatement selectStatement;

	/**
	 * The database connection
	 */
	private final Connection connection;

	public JDBCTupleStore() throws SQLException {
		connection = DriverManager.getConnection("jdbc:h2:nio:/tmp/dbtest.db" + DB_FLAGS);
		Statement statement = connection.createStatement();
		
		statement.executeUpdate("DROP TABLE if exists tuples");
		statement.executeUpdate("CREATE TABLE tuples (id BIGINT, data BLOB)");
		statement.close();
		
		insertStatement = connection.prepareStatement("INSERT into tuples (id, data) values (?,?)");
		selectStatement = connection.prepareStatement("SELECT data from tuples where id = ?");
	}

	@Override
	public void writeTuple(final Tuple tuple) throws Exception {
		final byte[] tupleBytes = TupleHelper.tupleToBytes(tuple);
		final InputStream is = new ByteArrayInputStream(tupleBytes);
		
		insertStatement.setLong(1, Integer.parseInt(tuple.getKey()));
		insertStatement.setBlob(2, is);
		insertStatement.execute();
	
		is.close();

		connection.commit();
	}

	@Override
	public Tuple readTuple(final String key) throws Exception {

		selectStatement.setLong(1, Integer.parseInt(key));
		final ResultSet result = selectStatement.executeQuery();
		
		if(! result.next()) {
			throw new RuntimeException("Unable to find node for way: " + key);
		}
		
		final byte[] bytes = result.getBytes(1);
		result.close();
		
		return TupleHelper.decodeTuple(ByteBuffer.wrap(bytes));
	}

	@Override
	public void close() throws Exception {
		if(insertStatement != null) {
			insertStatement.close();
		}
		
		if(selectStatement != null) {
			selectStatement.close();
		}
		
		if(connection != null) {
			connection.close();
		}	
	}
}
