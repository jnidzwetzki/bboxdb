package de.fernunihagen.dna.jkn.scalephant;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperClient;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperException;

public class TestZookeeperIntegration {


	/**
	 * Test the id generation
	 * @throws ZookeeperException
	 */
	@Test
	public void testTableIdGenerator() throws ZookeeperException {
		
		final ScalephantConfiguration scalephantConfiguration 
		     = ScalephantConfigurationManager.getConfiguration();
		
		final ZookeeperClient zookeeperClient 
		     = new ZookeeperClient(scalephantConfiguration.getZookeepernodes(), 
				                   scalephantConfiguration.getClustername());
		
		zookeeperClient.init();
		
		final List<Integer> ids = new ArrayList<Integer>();
		
		for(int i = 0; i < 10; i++) {
			int nextId = zookeeperClient.getNextTableIdForDistributionGroup("mygroup1");
			System.out.println("The next id is: " + nextId);
			
			Assert.assertFalse(ids.contains(nextId));
			ids.add(nextId);
		}
		
		zookeeperClient.shutdown();
	}
}
