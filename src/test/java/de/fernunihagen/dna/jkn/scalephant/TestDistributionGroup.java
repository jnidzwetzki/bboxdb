package de.fernunihagen.dna.jkn.scalephant;

import org.junit.Assert;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionRegion;

public class TestDistributionGroup {

	/**
	 * Create an illegal distibution group
	 */
	@Test(expected=IllegalArgumentException.class)
	public void createInvalidDistributionGroup1() {
		@SuppressWarnings("unused")
		final DistributionRegion distributionRegion = DistributionRegion.createRootRegion("foo");
	}
	
	/**
	 * Create an illegal distibution group
	 */
	@Test(expected=IllegalArgumentException.class)
	public void createInvalidDistributionGroup2() {
		@SuppressWarnings("unused")
		final DistributionRegion distributionRegion = DistributionRegion.createRootRegion("12_foo_bar");
	}
	
	/**
	 * Test a distribution region with only one level
	 */
	@Test
	public void testLeafNode() {
		final DistributionRegion distributionRegion = DistributionRegion.createRootRegion("3_foo");
		Assert.assertTrue(distributionRegion.isLeafRegion());
		Assert.assertEquals(3, distributionRegion.getDimension());
		Assert.assertEquals(0, distributionRegion.getLevel());
	}
	
	/**
	 * Test a distribution region with two levels level
	 */
	@Test
	public void testTwoLevel() {
		final DistributionRegion distributionRegion = DistributionRegion.createRootRegion("3_foo");
		Assert.assertTrue(distributionRegion.isLeafRegion());
		distributionRegion.setSplit(0);
		Assert.assertFalse(distributionRegion.isLeafRegion());

		Assert.assertEquals(distributionRegion, distributionRegion.getLeftChild().getRootRegion());
		Assert.assertEquals(distributionRegion, distributionRegion.getRightChild().getRootRegion());
		
		Assert.assertEquals(distributionRegion.getDimension(), distributionRegion.getLeftChild().getDimension());
		Assert.assertEquals(distributionRegion.getDimension(), distributionRegion.getRightChild().getDimension());
		
		Assert.assertEquals(1, distributionRegion.getLeftChild().getLevel());
		Assert.assertEquals(1, distributionRegion.getRightChild().getLevel());
	}
	
	/**
	 * Test the split dimension
	 */
	@Test
	public void testSplitDimension() {
		final DistributionRegion level0 = DistributionRegion.createRootRegion("2_foo");
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
		Assert.assertEquals(0, level2.getSplitDimension());
		Assert.assertEquals(1, level3.getSplitDimension());
		Assert.assertEquals(0, level4.getSplitDimension());
	}

}
