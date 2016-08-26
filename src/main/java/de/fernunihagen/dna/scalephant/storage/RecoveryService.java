package de.fernunihagen.dna.scalephant.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.ScalephantService;
import de.fernunihagen.dna.scalephant.distribution.membership.event.DistributedInstanceState;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClientFactory;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperException;

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
			// TODO: Do recovery
			zookeeperClient.setLocalInstanceState(DistributedInstanceState.READWRITE);
		} catch (ZookeeperException e) {
			logger.error("Got an exception during state update: ", e);
		}
	}

	@Override
	public void shutdown() {

	}

	@Override
	public String getServicename() {
		return "Recovery service";
	}

}
