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
package org.bboxdb.network.client;

import java.io.Closeable;
import java.util.List;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.JoinedTupleListFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;

public interface BBoxDB extends Closeable {
	
	/**
	 * Connect to the server
	 * @return true or false, depending on the connection state
	 */
	public boolean connect();

	/**
	 * Disconnect from the server
	 */
	@Override
	public void close();

	/**
	 * Create a new table
	 * @param table
	 * @param configuration
	 * @return
	 * @throws BBoxDBException
	 */
	public EmptyResultFuture createTable(final String table, final TupleStoreConfiguration configuration) throws BBoxDBException;
	
	/**
	 * Delete a table on the bboxdb server
	 * @param table
	 * @return
	 */
	public EmptyResultFuture deleteTable(final String table) throws BBoxDBException;

	/**
	 * Insert a new tuple into the given table
	 * @param tuple
	 * @param table
	 * @return
	 */
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple) throws BBoxDBException;

	/**
	 * Lock tuple
	 * @param table
	 * @param tuple
	 * @param deleteOnTimeout
	 * @return
	 */
	public EmptyResultFuture lockTuple(final String table, final Tuple tuple, 
			final boolean deleteOnTimeout) throws BBoxDBException;

	/**
	 * Delete the given key from a table
	 * @param table
	 * @param key
	 * @param timestamp
	 * @return
	 */
	public EmptyResultFuture deleteTuple(final String table, final String key, final long timestamp) throws BBoxDBException;

	/**
	 * Delete the given key from a table
	 * @param table
	 * @param key
	 * @param timestamp
	 * @return
	 */
	public EmptyResultFuture deleteTuple(final String table, final String key, final long timestamp, 
			final Hyperrectangle boundingBox) throws BBoxDBException;

	/**
	 * Delete the given key from a table - version without timestamp
	 * @param table
	 * @param key
	 * @return
	 * @throws BBoxDBException
	 */
	public EmptyResultFuture deleteTuple(String table, String key) throws BBoxDBException;

	/**
	 * Create a new distribution group
	 * @param distributionGroup
	 * @return
	 */
	public EmptyResultFuture createDistributionGroup(
			final String distributionGroup, final DistributionGroupConfiguration distributionGroupConfiguration) 
					throws BBoxDBException;

	/**
	 * Delete a distribution group
	 * @param distributionGroup
	 * @return
	 */
	public EmptyResultFuture deleteDistributionGroup(
			final String distributionGroup) throws BBoxDBException;

	/**
	 * Query the given table for a specific key
	 * @param table
	 * @param key
	 * @return
	 */
	public TupleListFuture queryKey(final String table, final String key) throws BBoxDBException;

	/**
	 * Execute a hyperrectangle query on the given table
	 * @param table
	 * @param boundingBox
	 * @return
	 */
	public TupleListFuture queryRectangle(final String table,
			final Hyperrectangle boundingBox) throws BBoxDBException;
	
	/**
	 * Execute a continuous bounding box query on the given table
	 * @param table
	 * @param boundingBox
	 * @return
	 * @throws BBoxDBException
	 */
	public TupleListFuture queryRectangleContinuous(final String table,
			final Hyperrectangle boundingBox) throws BBoxDBException;

	/**
	 * Query the given table for all tuples that have a newer version timestamp
	 * @param table
	 * @param key
	 * @return
	 */
	public TupleListFuture queryVersionTime(final String table, final long timestamp) throws BBoxDBException;

	/**
	 * Query the given table for all tuples that have a newer inserted timestamp
	 * @param table
	 * @param key
	 * @return
	 */
	public TupleListFuture queryInsertedTime(final String table, final long timestamp) throws BBoxDBException;
	
	/**
	 * Query the given table for all tuples newer than timestamp and inside of the bounding box
	 * @param table
	 * @param key
	 * @return
	 */
	public TupleListFuture queryRectangleAndTime(final String table, final Hyperrectangle boundingBox, final long timestamp) throws BBoxDBException;

	/**
	 * Execute a join
	 * @param tableNames
	 * @param boundingBox
	 * @return
	 */
	public JoinedTupleListFuture queryJoin(final List<String> tableNames, final Hyperrectangle boundingBox) throws BBoxDBException;
	
	/**
	 * Is the client connected?
	 * @return
	 */
	public boolean isConnected();

	/**
	 * Get the amount of in flight (running) calls
	 * @return
	 */
	public int getInFlightCalls();

	/**
	 * Is the paging for queries enables
	 * @return
	 */
	public boolean isPagingEnabled();

	/**
	 * Enable or disable paging
	 * @param pagingEnabled
	 */
	public void setPagingEnabled(final boolean pagingEnabled);

	/**
	 * Get the amount of tuples per page
	 * @return
	 */
	public short getTuplesPerPage();

	/**
	 * Set the tuples per page
	 * @param tuplesPerPage
	 */
	public void setTuplesPerPage(final short tuplesPerPage);

}