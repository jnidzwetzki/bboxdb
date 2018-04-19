package org.bboxdb.network;

import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.junit.Assert;

public class TestHelper {
	
	/**
	 * Recreate the given distribution group
	 * @param client
	 * @param DISTRIBUTION_GROUP
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	public static void recreateDistributionGroup(final BBoxDB client, final String DISTRIBUTION_GROUP) throws InterruptedException, BBoxDBException {
		
		// Delete distribution group
		final EmptyResultFuture resultDelete = client.deleteDistributionGroup(DISTRIBUTION_GROUP);
		resultDelete.waitForCompletion();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder.create(2)
				.withReplicationFactor((short) 1)
				.build();
		
		final EmptyResultFuture resultCreate = client.createDistributionGroup(DISTRIBUTION_GROUP, 
				configuration);
		
		resultCreate.waitForCompletion();
		Assert.assertFalse(resultCreate.isFailed());
	}
}
