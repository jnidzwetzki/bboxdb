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
package org.bboxdb.network.client.tools;

import java.util.List;
import java.util.Optional;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.TupleStoreAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreConfigurationBuilder;
import org.bboxdb.storage.entity.TupleStoreName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

public class IndexedTupleUpdateHelper {

	/**
	 * The reference to the BBoxDB cluster
	 */
	private final BBoxDBCluster cluster;

	/**
	 * The future store
	 */
	private final FixedSizeFutureStore futureStore;

	/**
	 * The suffix for the index table
	 */
	public final static String IDX_DGROUP_PREFIX ="#idx#";

	/**
	 * The default replication factor
	 */
	private final static short DEFAULT_REPLIATION_FACTOR = (short) 1;

	/**
	 * The max number of retries
	 */
	public final static int TOTAL_RETRIES = 10;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(IndexedTupleUpdateHelper.class);


	public IndexedTupleUpdateHelper(final BBoxDBCluster cluster) {
		this.cluster = cluster;
		this.futureStore = new FixedSizeFutureStore(10);

		// Log failed futures
		futureStore.addFailedFutureCallback(f -> logger.error("Failed future" + f.getAllMessages()));
	}

	/**
	 * Handle the tuple update (supported by indices)
	 * @param deletedTuple
	 * @return
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	public EmptyResultFuture handleTupleUpdate(final String table, final Tuple tuple)
			throws BBoxDBException, InterruptedException {

		try {
			// Might be full space covering box when index entry not found
			final Hyperrectangle oldBoundingBox = getAndLockOldBundingBoxForTuple(table, tuple);

			// Delete old tuple
			final String key = tuple.getKey();
			final long deletionTimestamp = tuple.getVersionTimestamp() - 1;
			final EmptyResultFuture deleteFuture = cluster.deleteTuple(
					table, key, deletionTimestamp, oldBoundingBox);
			futureStore.put(deleteFuture);

			// Insert new tuple
			final EmptyResultFuture insertFuture = cluster.insertTuple(table, tuple);
			futureStore.put(insertFuture);

			// Update index (and remove index locks)
			updateIndexEntry(table, tuple);

			return insertFuture;
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			throw new BBoxDBException(e);
		}
	}

	/**
	 * Update the bounding box index
	 * @param tuple
	 * @throws BBoxDBException
	 */
	private void updateIndexEntry(final String table, final Tuple tuple) throws BBoxDBException {
		final String boundingBoxValue = tuple.getBoundingBox().toCompactString();
		final String tablename = convertTablenameToIndexTablename(table);

		final String key = tuple.getKey();
		final Hyperrectangle indexBox = getBoundingBoxForKey(key);

		final Tuple tupleToUpdate = new Tuple(key, indexBox, boundingBoxValue.getBytes());
		final EmptyResultFuture insertFuture = cluster.insertTuple(tablename, tupleToUpdate);
		futureStore.put(insertFuture);
	}

	/**
	 * Get the bounding box for the given key
	 * @param key
	 * @return
	 */
	private Hyperrectangle getBoundingBoxForKey(final String key) {
		final int hashCode = key.hashCode();
		return new Hyperrectangle((double) hashCode, (double) hashCode);
	}

	/**
	 * Get the old bounding box for the tuple
	 * @param tuple
	 * @return
	 * @throws ZookeeperNotFoundException
	 * @throws ZookeeperException
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	private Hyperrectangle getAndLockOldBundingBoxForTuple(final String table, final Tuple tuple)
			throws ZookeeperException, ZookeeperNotFoundException, BBoxDBException, InterruptedException {

		final String indexTableName = createMissingTables(table);

		return tryToGetIndexEntry(indexTableName, tuple);
	}

	/**
	 * @param table
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	@VisibleForTesting
	public String createMissingTables(final String table)
			throws ZookeeperException, ZookeeperNotFoundException, BBoxDBException, InterruptedException {

		final ZookeeperClient zookeeperClient = cluster.getZookeeperClient();

		final String indexTableName = convertTablenameToIndexTablename(table);
		final TupleStoreName tupleStoreName = new TupleStoreName(indexTableName);

		createDistributionGroupIfMissing(zookeeperClient, tupleStoreName);
		createTableIfMissing(zookeeperClient, indexTableName, tupleStoreName);
		return indexTableName;
	}

	/**
	 * Get the old index entry
	 * @param tuple
	 * @param indexTableName
	 * @return
	 * @throws InterruptedException
	 * @throws BBoxDBException
	 */
	@VisibleForTesting
	public Optional<Tuple> getOldIndexEntry(final String indexTableName, final String key)
			throws InterruptedException, BBoxDBException {

		final Hyperrectangle boundingBox = getBoundingBoxForKey(key);
		final TupleListFuture resultFuture = cluster.queryRectangle(indexTableName, boundingBox);
		resultFuture.waitForCompletion();

		if(resultFuture.isFailed()) {
			logger.error("Index query future failed {}", resultFuture.getAllMessages());
			return Optional.empty();
		} else {
			final List<Tuple> resultList = Lists.newArrayList(resultFuture.iterator());

			final Optional<Tuple> indexEntry = resultList.stream()
					.filter(t -> t.getKey().equals(key))
					.findAny();

			if(indexEntry.isPresent()) {
				return indexEntry;
			} else {
				final Hyperrectangle fullSpace = Hyperrectangle.FULL_SPACE;
				final String fullSpaceString = fullSpace.toCompactString();
				return Optional.of(new Tuple(key, fullSpace, fullSpaceString.getBytes(), -1));
			}
		}
	}

	/**
	 * Try to get the index entry
	 * @param tuple
	 * @param indexTableName
	 * @return
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	private Hyperrectangle tryToGetIndexEntry(final String indexTableName, final Tuple tuple)
			throws BBoxDBException, InterruptedException {

		for(int i = 0; i < TOTAL_RETRIES; i++) {

			final Optional<Tuple> oldIndexEntryOpt = getOldIndexEntry(indexTableName, tuple.getKey());

			if(! oldIndexEntryOpt.isPresent()) {
				Thread.sleep(100);
				continue;
			}

			final Tuple oldIndexEntry = oldIndexEntryOpt.get();

			final EmptyResultFuture lockResult = cluster.lockTuple(indexTableName, oldIndexEntry, true);
			lockResult.waitForCompletion();

			// Lock was successful
			if(! lockResult.isFailed()) {
				final byte[] boundingBoxData = oldIndexEntry.getDataBytes();
				return new Hyperrectangle(new String(boundingBoxData));
			}
		}

		throw new BBoxDBException("Unable to lock index entry in " + TOTAL_RETRIES + " rounds");
	}


	/**
	 * Create the index table if missing
	 *
	 * @param zookeeperClient
	 * @param tablename
	 * @param tupleStoreName
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	private void createTableIfMissing(final ZookeeperClient zookeeperClient, final String tablename,
			final TupleStoreName tupleStoreName)
			throws ZookeeperException, ZookeeperNotFoundException, BBoxDBException, InterruptedException {

		final String distributionGroup = tupleStoreName.getDistributionGroup();

		final TupleStoreAdapter tupleStoreAdapter = new TupleStoreAdapter(zookeeperClient);
		final List<String> allTables = tupleStoreAdapter.getAllTables(distributionGroup);

		if(allTables.contains(tablename)) {
			return;
		}

		logger.info("Table {} not found, creating", tablename);
		final TupleStoreConfiguration tableconfig = TupleStoreConfigurationBuilder
				.create()
				.allowDuplicates(false)
				.build();

		final EmptyResultFuture createResult = cluster.createTable(tablename, tableconfig);
		createResult.waitForCompletion();

		if(createResult.isFailed()) {
			throw new BBoxDBException("Got an exception while creating table " + createResult.getAllMessages());
		}
	}

	/**
	 * Create the distribution group if missing
	 *
	 * @param zookeeperClient
	 * @param tupleStoreName
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	private void createDistributionGroupIfMissing(final ZookeeperClient zookeeperClient,
			final TupleStoreName tupleStoreName)
			throws ZookeeperException, ZookeeperNotFoundException, BBoxDBException, InterruptedException {

		final String distributionGroup = tupleStoreName.getDistributionGroup();

		final DistributionGroupAdapter distributionGroupAdapter = new DistributionGroupAdapter(zookeeperClient);

		final List<String> allGroups = distributionGroupAdapter.getDistributionGroups();

		if(allGroups.contains(distributionGroup)) {
			return;
		}

		logger.info("Distribution group {} not found, creating", distributionGroup);
		final DistributionGroupConfiguration dgroupConfig = DistributionGroupConfigurationBuilder
				.create(1)
				.withReplicationFactor(DEFAULT_REPLIATION_FACTOR)
				.build();

		final EmptyResultFuture dgroupFuture = cluster.createDistributionGroup(distributionGroup, dgroupConfig);
		dgroupFuture.waitForCompletion();

		if(dgroupFuture.isFailed()) {
			throw new BBoxDBException("Unable to create distribution group: " + dgroupFuture.getAllMessages());
		}
	}

	/**
	 * Get the name of the index table
	 *
	 * @param table
	 * @return
	 */
	@VisibleForTesting
	public static String convertTablenameToIndexTablename(final String table) {
		return IDX_DGROUP_PREFIX + table;
	}

	/**
	 * Wait for the completion of future operations
	 * @throws InterruptedException
	 */
	public void waitForCompletion() throws InterruptedException {
		futureStore.waitForCompletion();
	}

}
