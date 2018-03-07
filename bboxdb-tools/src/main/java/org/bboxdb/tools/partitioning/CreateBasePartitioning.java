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
package org.bboxdb.tools.partitioning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.bboxdb.commons.ListHelper;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.tools.TupleFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateBasePartitioning implements Runnable {

	/**
	 * The sampling file
	 */
	private final String filename;
	
	/**
	 * The format of the file
	 */
	private final String format;
	
	/**
	 * The distributin group
	 */
	private final DistributionGroupName distributionGroup;

	/**
	 * The number of partitons
	 */
	private final int partitions;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(CreateBasePartitioning.class);

	public CreateBasePartitioning(final String filename, final String format, 
			final DistributionGroupName distributionGroup, final int partitions) {
		
		this.filename = filename;
		this.format = format;
		this.distributionGroup = distributionGroup;
		this.partitions = partitions;
	}

	@Override
	public void run() {
		
		final TupleFileReader tupleFile = new TupleFileReader(filename, format);
		
		final List<BoundingBox> samples = new ArrayList<>();
		
		tupleFile.addTupleListener(t -> {
			final BoundingBox polygonBoundingBox = t.getBoundingBox();
			samples.add(polygonBoundingBox);
	    });
		
		try {
			tupleFile.processFile();
		
			final SpacePartitioner spacePartitioner = SpacePartitionerCache.getInstance()
					.getSpacePartitionerForGroupName(distributionGroup.getFullname());
			
			final DistributionGroupZookeeperAdapter adapter 
				= ZookeeperClientFactory.getDistributionGroupAdapter();
			
			while(getActiveRegions(spacePartitioner).size() < partitions) {
				logger.info("We have now {} of {} active partitons, executing split", 
						getActiveRegions(spacePartitioner).size() < partitions);
				
				final List<DistributionRegion> activeRegions = getActiveRegions(spacePartitioner);
				final DistributionRegion regionToSplit = ListHelper.getElementRandom(activeRegions);
				
				logger.info("Splitting region {}", regionToSplit.getRegionId());
				spacePartitioner.splitRegion(regionToSplit, samples);
				spacePartitioner.splitComplete(regionToSplit);
			}
			
			// Prevent merging of nodes
			for(DistributionRegion region : spacePartitioner.getRootNode().getAllChildren()) {
				adapter.setMergingSupported(region, false);
			}
		} catch (Exception e) {
			logger.error("Got an exception", e);
			System.exit(-1);
		}	
	}

	/**
	 * Get the active regions
	 * @param spacePartitioner
	 * @return
	 * @throws BBoxDBException
	 */
	private List<DistributionRegion> getActiveRegions(final SpacePartitioner spacePartitioner) 
			throws BBoxDBException {
		
		return spacePartitioner.getRootNode()
				.getThisAndChildRegions()
				.stream()
				.filter(r -> r.getState() == DistributionRegionState.ACTIVE)
				.collect(Collectors.toList());
	}
	
	public static void main(final String[] args) throws Exception {
		if(args.length != 5) {
			System.err.println("Usage: <File> <Format> <Distribution group> <Partitiones> "
					+ "<ZookeeperEndpoint> <Clustername>");
			System.exit(-1);
		}
		
		final String filename = args[0];
		final String format = args[1];
		final DistributionGroupName distributionGroup = new DistributionGroupName(args[2]);
		final int partitions = MathUtil.tryParseInt(args[3], () -> "Unable to parse: " + args[3]);
		final String zookeeperEndpoint = args[4];
		final String clustername = args[5];
		
		final ZookeeperClient zookeeperClient = new ZookeeperClient(
				Arrays.asList(zookeeperEndpoint), clustername);
		
		zookeeperClient.init();
		
		if(! zookeeperClient.isConnected()) {
			System.err.println("Unable to connect to zookeeper at: " + zookeeperEndpoint);
			System.exit(-1);
		}

		ZookeeperClientFactory.setDefaultZookeeperClient(zookeeperClient);
		doesGroupExist(distributionGroup);		
		checkForExistingPartitioning(distributionGroup);
		
		final CreateBasePartitioning dataRedistributionLoader = new CreateBasePartitioning(filename, format, 
				distributionGroup, partitions);
		
		dataRedistributionLoader.run();
	}

	/**
	 * Does the group exist?
	 * @param distributionGroup
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	private static void doesGroupExist(final DistributionGroupName distributionGroup)
			throws ZookeeperException, ZookeeperNotFoundException {
		
		final DistributionGroupZookeeperAdapter adapter = ZookeeperClientFactory.getDistributionGroupAdapter();
		
		final List<DistributionGroupName> knownGroups = adapter.getDistributionGroups();
		
		if(! knownGroups.contains(distributionGroup)) {
			System.err.format("Distribution group %s does not exist\n", distributionGroup.getFullname());
			System.exit(-1);
		}
	}

	/**
	 * Is the region already partiitoned?
	 * @param distributionGroup
	 * @throws BBoxDBException
	 */
	private static void checkForExistingPartitioning(final DistributionGroupName distributionGroup)
			throws BBoxDBException {
		
		final SpacePartitioner spacePartitioner = SpacePartitionerCache.getInstance()
				.getSpacePartitionerForGroupName(distributionGroup.getFullname());
		
		if(spacePartitioner.getRootNode().getThisAndChildRegions().size() != 1) {
			System.err.println("Region is already splitted unable to use this for inital splitting");
			System.exit(-1);
		}
	}

}
