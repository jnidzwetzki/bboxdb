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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.util.TupleHelper;

public abstract class AbstractJDBCTupleStore implements TupleStore {
	
    /**
     * The insert statement
     */
	private PreparedStatement insertStatement;
	
	/**
	 * The select statement
	 */
	private PreparedStatement selectStatement;

	/**
	 * The database connection
	 */
	private Connection connection;

	/**
	 * The database dir
	 */
	protected File dir;

	public AbstractJDBCTupleStore(final File dir) {
		this.dir = dir;
	}
	
	protected abstract String getConnectionURL();

	@Override
	public void writeTuple(final Tuple tuple) throws Exception {
		final byte[] tupleBytes = TupleHelper.tupleToBytes(tuple);
		final InputStream is = new ByteArrayInputStream(tupleBytes);
		
		insertStatement.setInt(1, Integer.parseInt(tuple.getKey()));
		insertStatement.setBlob(2, is);
		insertStatement.execute();
	
		is.close();

		connection.commit();
	}

	@Override
	public Tuple readTuple(final String key) throws Exception {

		selectStatement.setInt(1, Integer.parseInt(key));
		final ResultSet result = selectStatement.executeQuery();
		
		if(! result.next()) {
			throw new RuntimeException("Unable to find data for key: " + key);
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
		
		afterShutdownHook();
	}
	
	/**
	 * The after shutdown hook
	 */
	public void afterShutdownHook() {
		// Default: do nothing
	}
	
	/**
	 * The create table SQL statement
	 * @return
	 */
	public abstract String getCreateTableSQL();
	
	/**
	 * The DB file
	 * @return
	 */
	public abstract File getDBFile();
	
	@Override
	public void open() throws Exception {
		final boolean newCreated = ! getDBFile().exists();
			
		connection = DriverManager.getConnection(getConnectionURL());
		
		if(newCreated) {
			try(final Statement statement = connection.createStatement()) {	
				statement.executeUpdate(getCreateTableSQL());
				statement.close();
				connection.commit();
			} catch(SQLException e) {
				throw e;
			}
 		}
		
		insertStatement = connection.prepareStatement("INSERT into tuples (id, data) values (?,?)");
		selectStatement = connection.prepareStatement("SELECT data from tuples where id = ?");
	}
}
