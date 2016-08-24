package de.fernunihagen.dna.scalephant.network.client;

import de.fernunihagen.dna.scalephant.network.NetworkConnectionState;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;

public interface Scalephant {
	
	/**
	 * The maximum amount of in flight requests. Needs to be lower than Short.MAX_VALUE to
	 * prevent two in flight requests with the same id.
	 */
	public final static short MAX_IN_FLIGHT_CALLS = 1000;

	/**
	 * Connect to the server
	 * @return true or false, depending on the connection state
	 */
	public abstract boolean connect();

	/**
	 * Disconnect from the server
	 */
	public abstract boolean disconnect();

	/**
	 * Delete a table on the scalephant server
	 * @param table
	 * @return
	 */
	public abstract OperationFuture deleteTable(final String table);

	/**
	 * Insert a new tuple into the given table
	 * @param tuple
	 * @param table
	 * @return
	 */
	public abstract OperationFuture insertTuple(final String table, final Tuple tuple);

	/**
	 * Delete the given key from a table
	 * @param table
	 * @param key
	 * @return
	 */
	public abstract OperationFuture deleteTuple(final String table, final String key);

	/**
	 * List the existing tables
	 * @return
	 */
	public abstract OperationFuture listTables();

	/**
	 * Create a new distribution group
	 * @param distributionGroup
	 * @return
	 */
	public abstract OperationFuture createDistributionGroup(
			final String distributionGroup, final short replicationFactor);

	/**
	 * Delete a distribution group
	 * @param distributionGroup
	 * @return
	 */
	public abstract OperationFuture deleteDistributionGroup(
			final String distributionGroup);

	/**
	 * Query the given table for a specific key
	 * @param table
	 * @param key
	 * @return
	 */
	public abstract OperationFuture queryKey(final String table, final String key);

	/**
	 * Execute a bounding box query on the given table
	 * @param table
	 * @param boundingBox
	 * @return
	 */
	public abstract OperationFuture queryBoundingBox(final String table,
			final BoundingBox boundingBox);

	/**
	 * Query the given table for all tuples newer than
	 * @param table
	 * @param key
	 * @return
	 */
	public abstract OperationFuture queryTime(final String table, final long timestamp);

	/**
	 * Is the client connected?
	 * @return
	 */
	public abstract boolean isConnected();

	/**
	 * Returns the state of the connection
	 * @return
	 */
	public abstract NetworkConnectionState getConnectionState();

	/**
	 * Get the amount of in flight (running) calls
	 * @return
	 */
	public abstract int getInFlightCalls();

	/**
	 * Get the max amount of in flight calls
	 * @return
	 */
	public abstract short getMaxInFlightCalls();

	/**
	 * Set the max amount of in flight calls
	 * @param maxInFlightCalls
	 */
	public abstract void setMaxInFlightCalls(final short maxInFlightCalls);

}