package de.fernunihagen.dna.jkn.scalephant;

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionRegionFactory;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;

public class TestDistributionGroup {

	/**
	 * Create an illegal distribution group
	 */
	@Test(expected=IllegalArgumentException.class)
	public void createInvalidDistributionGroup1() {
		@SuppressWarnings("unused")
		final DistributionRegion distributionRegion = DistributionRegionFactory.createRootRegion("foo");
	}
	
	/**
	 * Create an illegal distribution group
	 */
	@Test(expected=IllegalArgumentException.class)
	public void createInvalidDistributionGroup2() {
		@SuppressWarnings("unused")
		final DistributionRegion distributionRegion = DistributionRegionFactory.createRootRegion("12_foo_bar");
	}
	
	/**
	 * Test a distribution region with only one level
	 */
	@Test
	public void testLeafNode() {
		final DistributionRegion distributionRegion = DistributionRegionFactory.createRootRegion("3_foo");
		Assert.assertTrue(distributionRegion.isLeafRegion());
		Assert.assertEquals(3, distributionRegion.getDimension());
		Assert.assertEquals(0, distributionRegion.getLevel());
		
		Assert.assertEquals(1, distributionRegion.getTotalLevel());
	}
	
	/**
	 * Test a distribution region with two levels level
	 */
	@Test
	public void testTwoLevel() {
		final DistributionRegion distributionRegion = DistributionRegionFactory.createRootRegion("3_foo");
		Assert.assertTrue(distributionRegion.isLeafRegion());
		distributionRegion.setSplit(0);
		Assert.assertFalse(distributionRegion.isLeafRegion());

		Assert.assertEquals(distributionRegion, distributionRegion.getLeftChild().getRootRegion());
		Assert.assertEquals(distributionRegion, distributionRegion.getRightChild().getRootRegion());
		
		Assert.assertEquals(distributionRegion.getDimension(), distributionRegion.getLeftChild().getDimension());
		Assert.assertEquals(distributionRegion.getDimension(), distributionRegion.getRightChild().getDimension());
		
		Assert.assertEquals(1, distributionRegion.getLeftChild().getLevel());
		Assert.assertEquals(1, distributionRegion.getRightChild().getLevel());
		
		Assert.assertEquals(2, distributionRegion.getTotalLevel());
	}
	
	/**
	 * Test the split dimension 2d
	 */
	@Test
	public void testSplitDimension1() {
		final DistributionRegion level0 = DistributionRegionFactory.createRootRegion("2_foo");
		level0.setSplit(50);
		final DistributionRegion level1 = level0.getLeftChild();
		level1.setSplit(40);
		final DistributionRegion level2 = level1.getLeftChild();
		level2.setSplit(-30);
		final DistributionRegion level3 = level2.getLeftChild();
		level3.setSplit(30);
		final DistributionRegion level4 = level3.getLeftChild();

		Assert.assertEquals(0, level0.getSplitDimension());
		Assert.assertEquals(1, level1.getSplitDimension());
		Assert.assertEquals(0, level2.getSplitDimension());
		Assert.assertEquals(1, level3.getSplitDimension());
		Assert.assertEquals(0, level4.getSplitDimension());
	}
	
	/**
	 * Test the split dimension 3d
	 */
	@Test
	public void testSplitDimension2() {
		final DistributionRegion level0 = DistributionRegionFactory.createRootRegion("3_foo");
		level0.setSplit(50);
		final DistributionRegion level1 = level0.getLeftChild();
		level1.setSplit(40);
		final DistributionRegion level2 = level1.getLeftChild();
		level2.setSplit(30);
		final DistributionRegion level3 = level2.getLeftChild();
		level3.setSplit(30);
		final DistributionRegion level4 = level3.getLeftChild();

		Assert.assertEquals(0, level0.getSplitDimension());
		Assert.assertEquals(1, level1.getSplitDimension());
		Assert.assertEquals(2, level2.getSplitDimension());
		Assert.assertEquals(0, level3.getSplitDimension());
		Assert.assertEquals(1, level4.getSplitDimension());
		
		Assert.assertEquals(5, level0.getTotalLevel());
		Assert.assertEquals(5, level1.getTotalLevel());
		Assert.assertEquals(5, level2.getTotalLevel());
		Assert.assertEquals(5, level3.getTotalLevel());
		Assert.assertEquals(5, level4.getTotalLevel());
	}
	
	/**
	 * Test isLeftChild and isRightChild method
	 */
	@Test
	public void testLeftOrRightChild() {
		final DistributionRegion level0 = DistributionRegionFactory.createRootRegion("3_foo");
		Assert.assertFalse(level0.isLeftChild());
		Assert.assertFalse(level0.isRightChild());
		
		level0.setSplit(50);
				
		Assert.assertTrue(level0.getLeftChild().isLeftChild());
		Assert.assertTrue(level0.getRightChild().isRightChild());
		Assert.assertFalse(level0.getRightChild().isLeftChild());
		Assert.assertFalse(level0.getLeftChild().isRightChild());
	}

	/**
	 * Test the find systems method
	 */
	@Test
	public void testFindSystems() {
		final DistributedInstance SYSTEM_A = new DistributedInstance("192.168.1.200:5050");
		final DistributedInstance SYSTEM_B = new DistributedInstance("192.168.1.201:5050");
		
		final DistributionRegion level0 = DistributionRegionFactory.createRootRegion("1_foo");
		level0.setSplit(50);

		level0.getLeftChild().addSystem(SYSTEM_A);
		level0.getRightChild().addSystem(SYSTEM_B);
		
		Assert.assertFalse(level0.getSystemsForBoundingBox(new BoundingBox(100f, 110f)).contains(SYSTEM_A));
		Assert.assertTrue(level0.getSystemsForBoundingBox(new BoundingBox(0f, 10f)).contains(SYSTEM_A));
		
		Assert.assertTrue(level0.getSystemsForBoundingBox(new BoundingBox(0f, 10f)).contains(SYSTEM_A));
		Assert.assertFalse(level0.getSystemsForBoundingBox(new BoundingBox(100f, 110f)).contains(SYSTEM_A));
		
		Assert.assertTrue(level0.getSystemsForBoundingBox(new BoundingBox(0f, 100f)).contains(SYSTEM_A));
		Assert.assertTrue(level0.getSystemsForBoundingBox(new BoundingBox(0f, 100f)).contains(SYSTEM_B));
	}
	
	/**
	 * Test nameprefix search
	 */
	@Test
	public void testNameprefixSearch() {
		final DistributionRegion level0 = DistributionRegionFactory.createRootRegion("2_foo");
		level0.setNameprefix(1);
		level0.setSplit(50);
		level0.getLeftChild().setNameprefix(2);
		level0.getRightChild().setNameprefix(3);
		
		final DistributionRegion level1 = level0.getLeftChild();
		level1.setSplit(40);
		level1.getLeftChild().setNameprefix(4);
		level1.getRightChild().setNameprefix(5);
		
		final DistributionRegion level2 = level1.getLeftChild();
		level2.setSplit(30);
		level2.getLeftChild().setNameprefix(6);
		level2.getRightChild().setNameprefix(7);
		
		final DistributionRegion level3 = level2.getLeftChild();
		level3.setSplit(35);
		level3.getLeftChild().setNameprefix(8);
		level3.getRightChild().setNameprefix(9);
		
		Assert.assertTrue(level0.getDistributionRegionForNamePrefix(4711) == null);
		
		Assert.assertEquals(level0, level0.getDistributionRegionForNamePrefix(1));
		Assert.assertEquals(level1.getLeftChild(), level0.getDistributionRegionForNamePrefix(4));
		Assert.assertEquals(level1.getRightChild(), level0.getDistributionRegionForNamePrefix(5));
		Assert.assertEquals(level3.getLeftChild(), level0.getDistributionRegionForNamePrefix(8));
		Assert.assertEquals(level3.getRightChild(), level0.getDistributionRegionForNamePrefix(9));
		
		Assert.assertEquals(null, level3.getDistributionRegionForNamePrefix(1));
	}
	
	
	/**
	 * Get the systems for a distribution group
	 */
	@Test
	public void testGetSystemsForDistributionGroup1() {
		final DistributionRegion level0 = DistributionRegionFactory.createRootRegion("2_foo");
		level0.setSplit(50);
		level0.getLeftChild().setNameprefix(2);
		level0.getRightChild().setNameprefix(3);
		
		level0.addSystem(new DistributedInstance("node1:123"));
		level0.getLeftChild().addSystem(new DistributedInstance("node2:123"));
		level0.getRightChild().addSystem(new DistributedInstance("node3:123"));
		
		final List<DistributedInstance> systems = level0.getSystemsForBoundingBox(BoundingBox.EMPTY_BOX);
		Assert.assertEquals(2, systems.size());
	}
	
	/**
	 * Get the systems for a distribution group
	 */
	@Test
	public void testGetSystemsForDistributionGroup2() {
		final DistributionRegion level0 = DistributionRegionFactory.createRootRegion("2_foo");
		level0.setSplit(50);
		level0.getLeftChild().setNameprefix(2);
		level0.getRightChild().setNameprefix(3);
		
		level0.addSystem(new DistributedInstance("node1:123"));
		level0.getLeftChild().addSystem(new DistributedInstance("node2:123"));
		level0.getRightChild().addSystem(new DistributedInstance("node2:123"));
		
		final List<DistributedInstance> systems = level0.getSystemsForBoundingBox(BoundingBox.EMPTY_BOX);
		Assert.assertEquals(1, systems.size());
	}
	
	/**
	 * Get the distribution groups for a distribution group
	 */
	@Test
	public void testGetDistributionGroupsForDistributionGroup() {
		final DistributionRegion level0 = DistributionRegionFactory.createRootRegion("2_foo");
		level0.setSplit(50);
		level0.getLeftChild().setNameprefix(2);
		level0.getRightChild().setNameprefix(3);
		
		level0.addSystem(new DistributedInstance("node1:123"));
		level0.getLeftChild().addSystem(new DistributedInstance("node2:123"));
		level0.getRightChild().addSystem(new DistributedInstance("node2:123"));
		
		final Set<DistributionRegion> regions = level0.getDistributionRegionsForBoundingBox(BoundingBox.EMPTY_BOX);
		Assert.assertEquals(3, regions.size());
	}
}
