/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.network.packages.request;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.bboxdb.misc.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.routing.RoutingHeader;

public class CreateDistributionGroupRequest extends NetworkRequestPackage {
	
	/**
	 * The name of the table
	 */
	protected final String distributionGroup;
	
	/**
	 * The max size of the region
	 */
	protected final int regionSize;
	
	/**
	 * The replication factor for the distribution group
	 */
	protected final short replicationFactor;

	/** 
	 * The placement strategy
	 */
	protected final String placementStrategy;
	
	/**
	 * The placement strategy config
	 */
	protected final String placementStrategyConfig;

	/**
	 * The space partitioner
	 */
	protected final String spacePartitioner;
	
	/**
	 * The space partitioner configuration
	 */
	protected final String spacePartitionerConfig;


	public CreateDistributionGroupRequest(final short sequencNumber,
			final String distributionGroup, final short replicationFactor,
			final int regionSize, final String placementStrategy, 
			final String placementStrategyConfig, final String spacePartitioner, 
			final String spacePartitionerConfig) {
		
		super(sequencNumber);
		
		this.distributionGroup = distributionGroup;
		this.regionSize = regionSize;
		this.replicationFactor = replicationFactor;
		this.placementStrategy = placementStrategy;
		this.placementStrategyConfig = placementStrategyConfig;
		this.spacePartitioner = spacePartitioner;
		this.spacePartitionerConfig = spacePartitionerConfig;
	}
	
	@Override
	public void writeToOutputStream(final OutputStream outputStream) throws PackageEncodeException {

		try {
			final byte[] groupBytes = distributionGroup.getBytes();
			final byte[] placementBytes = placementStrategy.getBytes();
			final byte[] placementConfigBytes = placementStrategyConfig.getBytes();
			final byte[] spacePartitionierBytes = spacePartitioner.getBytes();
			final byte[] spacePartitionierConfigBytes = spacePartitionerConfig.getBytes();

			final ByteBuffer bb = ByteBuffer.allocate(20);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			bb.putShort(replicationFactor);
			bb.putShort((short) groupBytes.length);
			bb.putShort((short) placementBytes.length);
			bb.putShort((short) spacePartitionierBytes.length);
			bb.putInt((int) placementConfigBytes.length);
			bb.putInt((int) spacePartitionierConfigBytes.length);
			bb.putInt(regionSize);
			
			// Body length
			final long bodyLength = bb.capacity() + groupBytes.length 
					+ placementBytes.length + placementConfigBytes.length 
					+ spacePartitionierBytes.length
					+ spacePartitionierConfigBytes.length;

			// Unrouted package
			final RoutingHeader routingHeader = new RoutingHeader(false);
			appendRequestPackageHeader(bodyLength, routingHeader, outputStream);
			
			// Write body
			outputStream.write(bb.array());
			outputStream.write(groupBytes);	
			outputStream.write(placementBytes);		
			outputStream.write(placementConfigBytes);			
			outputStream.write(spacePartitionierBytes);			
			outputStream.write(spacePartitionierConfigBytes);			

		} catch (IOException e) {
			throw new PackageEncodeException("Got exception while converting package into bytes", e);
		}
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PackageEncodeException 
	 */
	public static CreateDistributionGroupRequest decodeTuple(final ByteBuffer encodedPackage) throws PackageEncodeException {
		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromRequestPackage(encodedPackage);

		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_CREATE_DISTRIBUTION_GROUP);
		
		if(decodeResult == false) {
			throw new PackageEncodeException("Unable to decode package");
		}
		
		final short replicationFactor = encodedPackage.getShort();
		final short groupLength = encodedPackage.getShort();
		final short placementLength = encodedPackage.getShort();
		final short spacePartitionerLength = encodedPackage.getShort();
		final int placementConfigLength = encodedPackage.getInt();
		final int spacePartitionerConfigLength = encodedPackage.getInt();
		final int regionSize = encodedPackage.getInt();
		
		// Distribution group
		final byte[] groupBytes = new byte[groupLength];
		encodedPackage.get(groupBytes, 0, groupBytes.length);
		final String distributionGroup = new String(groupBytes);
		
		// Placement strategy
		final byte[] placementBytes = new byte[placementLength];
		encodedPackage.get(placementBytes, 0, placementBytes.length);
		final String placemeneStrategy = new String(placementBytes);
		
		// Placement config length
		final byte[] placementConfigBytes = new byte[placementConfigLength];
		encodedPackage.get(placementConfigBytes, 0, placementConfigBytes.length);
		final String placementConfig = new String(placementConfigBytes);
		
		// Space partitioner
		final byte[] spacePartitionerBytes = new byte[spacePartitionerLength];
		encodedPackage.get(spacePartitionerBytes, 0, spacePartitionerBytes.length);
		final String spacePartitioner = new String(spacePartitionerBytes);
		
		// Space partitioner configuration
		final byte[] spacePartitionerConfigBytes = new byte[spacePartitionerConfigLength];
		encodedPackage.get(spacePartitionerConfigBytes, 0, spacePartitionerConfigBytes.length);
		final String spacePartitionerConfig = new String(spacePartitionerConfigBytes);
		
		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new CreateDistributionGroupRequest(sequenceNumber, distributionGroup, replicationFactor, 
				regionSize, placemeneStrategy, placementConfig, spacePartitioner, spacePartitionerConfig);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_CREATE_DISTRIBUTION_GROUP;
	}

	public String getDistributionGroup() {
		return distributionGroup;
	}
	
	public short getReplicationFactor() {
		return replicationFactor;
	}

	public String getPlacementStrategy() {
		return placementStrategy;
	}
	
	public String getPlacementStrategyConfig() {
		return placementStrategyConfig;
	}

	public int getRegionSize() {
		return regionSize;
	}
	
	public String getSpacePartitioner() {
		return spacePartitioner;
	}
	
	public String getSpacePartitionerConfig() {
		return spacePartitionerConfig;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((distributionGroup == null) ? 0 : distributionGroup.hashCode());
		result = prime * result + ((placementStrategy == null) ? 0 : placementStrategy.hashCode());
		result = prime * result + ((placementStrategyConfig == null) ? 0 : placementStrategyConfig.hashCode());
		result = prime * result + regionSize;
		result = prime * result + replicationFactor;
		result = prime * result + ((spacePartitioner == null) ? 0 : spacePartitioner.hashCode());
		result = prime * result + ((spacePartitionerConfig == null) ? 0 : spacePartitionerConfig.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CreateDistributionGroupRequest other = (CreateDistributionGroupRequest) obj;
		if (distributionGroup == null) {
			if (other.distributionGroup != null)
				return false;
		} else if (!distributionGroup.equals(other.distributionGroup))
			return false;
		if (placementStrategy == null) {
			if (other.placementStrategy != null)
				return false;
		} else if (!placementStrategy.equals(other.placementStrategy))
			return false;
		if (placementStrategyConfig == null) {
			if (other.placementStrategyConfig != null)
				return false;
		} else if (!placementStrategyConfig.equals(other.placementStrategyConfig))
			return false;
		if (regionSize != other.regionSize)
			return false;
		if (replicationFactor != other.replicationFactor)
			return false;
		if (spacePartitioner == null) {
			if (other.spacePartitioner != null)
				return false;
		} else if (!spacePartitioner.equals(other.spacePartitioner))
			return false;
		if (spacePartitionerConfig == null) {
			if (other.spacePartitionerConfig != null)
				return false;
		} else if (!spacePartitionerConfig.equals(other.spacePartitionerConfig))
			return false;
		return true;
	}

	
}
