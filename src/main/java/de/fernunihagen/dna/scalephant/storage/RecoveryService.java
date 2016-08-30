package de.fernunihagen.dna.scalephant.storage;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.scalephant.ScalephantService;
import de.fernunihagen.dna.scalephant.distribution.DistributionGroupCache;
import de.fernunihagen.dna.scalephant.distribution.DistributionGroupName;
import de.fernunihagen.dna.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.scalephant.distribution.DistributionRegionHelper;
import de.fernunihagen.dna.scalephant.distribution.OutdatedDistributionRegion;
import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.scalephant.distribution.membership.MembershipConnectionService;
import de.fernunihagen.dna.scalephant.distribution.membership.event.DistributedInstanceState;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClientFactory;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperException;
import de.fernunihagen.dna.scalephant.network.client.ClientOperationFuture;
import de.fernunihagen.dna.scalephant.network.client.ScalephantClient;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableManager;

public class RecoveryService implements ScalephantService {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RecoveryService.class);
	

	public RecoveryService() {

	}

	@Override
	public void init() {
		try {
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClientAndInit();
			zookeeperClient.setLocalInstanceState(DistributedInstanceState.READONLY);
			logger.info("Running recovery for local stored data");
			
			runRecovery();
			
			logger.info("Running recovery for local stored data DONE");
			zookeeperClient.setLocalInstanceState(DistributedInstanceState.READWRITE);
		} catch (ZookeeperException e) {
			logger.error("Got an exception during recovery: ", e);
		}
	}

	/**
	 * Run the recovery
	 * @throws ZookeeperException 
	 */
	protected void runRecovery() throws ZookeeperException {
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClientAndInit();
		final List<DistributionGroupName> distributionGroups = zookeeperClient.getDistributionGroups();
		for(final DistributionGroupName distributionGroupName : distributionGroups) {
			logger.info("Running recovery for distribution group: " + distributionGroupName);
			runRecoveryForDistributionGroup(distributionGroupName);
		}
	}

	/**
	 * Run recovery for distribution group
	 * @param distributionGroupName
	 * @throws ZookeeperException 
	 */
	protected void runRecoveryForDistributionGroup(final DistributionGroupName distributionGroupName) {
		try {
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClientAndInit();
			final ScalephantConfiguration scalephantConfiguration = ScalephantConfigurationManager.getConfiguration();
			final DistributedInstance localInstance = ZookeeperClientFactory.getLocalInstanceName(scalephantConfiguration);
			final DistributionRegion distributionGroup = DistributionGroupCache.getGroupForGroupName(distributionGroupName.getFullname(), zookeeperClient);
		
			final List<OutdatedDistributionRegion> outdatedRegions = DistributionRegionHelper.getOutdatedRegions(distributionGroup, localInstance);
			handleOutdatedRegions(distributionGroupName, outdatedRegions);
		} catch (ZookeeperException e) {
			logger.error("Got exception while running recovery for distribution group: " + distributionGroupName, e);
		}
		
	}

	/**
	 * Handle the outdated distribution regions
	 * @param distributionGroupName
	 * @param outdatedRegions
	 */
	protected void handleOutdatedRegions(final DistributionGroupName distributionGroupName, final List<OutdatedDistributionRegion> outdatedRegions) {
		for(final OutdatedDistributionRegion outdatedDistributionRegion : outdatedRegions) {
			
			final ScalephantClient connection = MembershipConnectionService.getInstance()
					.getConnectionForInstance(outdatedDistributionRegion.getNewestInstance());
			
			final List<SSTableName> allTables = StorageInterface
					.getAllTablesForNameprefix(outdatedDistributionRegion.getDistributedRegion().getNameprefix());
			
			for(final SSTableName ssTableName : allTables) {
				try {
					runRecoveryForTable(ssTableName, outdatedDistributionRegion, connection);
				} catch (Exception e) {
					logger.error("Got an exception while performing recovery for table: " + ssTableName.getFullname());
				}
			}
		}
	}

	/**
	 * Run the recovery for a given table
	 * @param ssTableName
	 * @param outdatedDistributionRegion
	 * @param connection
	 * @throws StorageManagerException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	protected void runRecoveryForTable(final SSTableName ssTableName,
			final OutdatedDistributionRegion outdatedDistributionRegion,
			final ScalephantClient connection) throws StorageManagerException,
			InterruptedException, ExecutionException {
		
		logger.info("Starting recovery for table: " + ssTableName.getFullname());
		final SSTableManager tableManager = StorageInterface.getSSTableManager(ssTableName);
		final ClientOperationFuture result = connection.queryTime(ssTableName.getFullname(), outdatedDistributionRegion.getLocalVersion());
		result.waitForAll();
		
		if(result.isFailed()) {
			logger.warn("Failed result - Some tuples could not be received!");
			return;
		}
		
		for(int resultId = 0; resultId < result.getNumberOfResultObjets(); resultId++) {
			
			long insertedTuples = 0;
			@SuppressWarnings("unchecked")
			final List<Tuple> queryResult = (List<Tuple>) result.get(resultId);
			for(final Tuple tuple : queryResult) {
				tableManager.put(tuple);
				insertedTuples++;
			}
			
			logger.info("Inserted " + insertedTuples + " tuples into table " + ssTableName.getFullname());
		}
	}

	@Override
	public void shutdown() {
		// Nothing to do
	}

	@Override
	public String getServicename() {
		return "Recovery service";
	}

}
