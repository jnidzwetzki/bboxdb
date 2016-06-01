package de.fernunihagen.dna.jkn.scalephant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperClient;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperException;

public class TestZookeeperIntegration {

	/**
	 * The zookeeper client
	 */
	protected  ZookeeperClient zookeeperClient;
	
	@Before
	public void before() {
		final ScalephantConfiguration scalephantConfiguration 
			= ScalephantConfigurationManager.getConfiguration();
	
		final Collection<String> zookeepernodes = scalephantConfiguration.getZookeepernodes();
		final String clustername = scalephantConfiguration.getClustername();

		System.out.println("Zookeeper nodes are: " + zookeepernodes);
		System.out.println("Zookeeper cluster is: " + clustername);
	
		zookeeperClient = new ZookeeperClient(zookeepernodes, clustername);
		zookeeperClient.init();
	}
	
	@After
	public void after() {
		zookeeperClient.shutdown();
	}

	/**
	 * Test the id generation
	 * @throws ZookeeperException
	 */
	@Test
	public void testTableIdGenerator() throws ZookeeperException {
		final List<Integer> ids = new ArrayList<Integer>();
		
		for(int i = 0; i < 10; i++) {
			int nextId = zookeeperClient.getNextTableIdForDistributionGroup("mygroup1");
			System.out.println("The next id is: " + nextId);
			
			Assert.assertFalse(ids.contains(nextId));
			ids.add(nextId);
		}
		
	}
}
