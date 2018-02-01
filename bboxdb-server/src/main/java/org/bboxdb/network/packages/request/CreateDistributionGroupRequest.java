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
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;

public class CreateDistributionGroupRequest extends NetworkRequestPackage {
	
	/**
	 * The name of the table
	 */
	protected final String distributionGroup;
	
	/**
	 * The distribution group configuration
	 */
	protected final DistributionGroupConfiguration distributionGroupConfiguration;

	public CreateDistributionGroupRequest(final short sequencNumber,
			final String distributionGroup, 
			final DistributionGroupConfiguration distributionGroupConfiguration) {
		
		super(sequencNumber);
		
		this.distributionGroup = distributionGroup;
		this.distributionGroupConfiguration = distributionGroupConfiguration;
	}
	
	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PackageEncodeException {

		try {
			final byte[] groupBytes = distributionGroup.getBytes();
			final byte[] placementBytes = distributionGroupConfiguration.getPlacementStrategy().getBytes();
			final byte[] placementConfigBytes = distributionGroupConfiguration.getPlacementStrategyConfig().getBytes();
			final byte[] spacePartitionierBytes = distributionGroupConfiguration.getSpacePartitioner().getBytes();
			final byte[] spacePartitionierConfigBytes = distributionGroupConfiguration.getSpacePartitionerConfig().getBytes();

			final ByteBuffer bb = ByteBuffer.allocate(28);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			bb.putInt(distributionGroupConfiguration.getDimensions());
			bb.putShort(distributionGroupConfiguration.getReplicationFactor());
			bb.putShort((short) groupBytes.length);
			bb.putShort((short) placementBytes.length);
			bb.putShort((short) spacePartitionierBytes.length);
			bb.putInt((int) placementConfigBytes.length);
			bb.putInt((int) spacePartitionierConfigBytes.length);
			bb.putInt(distributionGroupConfiguration.getMaximumRegionSize());
			bb.putInt(distributionGroupConfiguration.getMinimumRegionSize());

			// Body length
			final long bodyLength = bb.capacity() + groupBytes.length 
					+ placementBytes.length + placementConfigBytes.length 
					+ spacePartitionierBytes.length
					+ spacePartitionierConfigBytes.length;

			// Unrouted package
			final RoutingHeader routingHeader = new RoutingHeader(false);
			final long headerLength = appendRequestPackageHeader(bodyLength, routingHeader, outputStream);
			
			// Write body
			outputStream.write(bb.array());
			outputStream.write(groupBytes);	
			outputStream.write(placementBytes);		
			outputStream.write(placementConfigBytes);			
			outputStream.write(spacePartitionierBytes);			
			outputStream.write(spacePartitionierConfigBytes);			

			return headerLength + bodyLength;
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
		
		final int dimensions = encodedPackage.getInt();
		final short replicationFactor = encodedPackage.getShort();
		final short groupLength = encodedPackage.getShort();
		final short placementLength = encodedPackage.getShort();
		final short spacePartitionerLength = encodedPackage.getShort();
		final int placementConfigLength = encodedPackage.getInt();
		final int spacePartitionerConfigLength = encodedPackage.getInt();
		final int maximumRegionSize = encodedPackage.getInt();
		final int minimumRegionSize = encodedPackage.getInt();
		
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
		
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder.create(dimensions)
				.withPlacementStrategy(placemeneStrategy, placementConfig)
				.withSpacePartitioner(spacePartitioner, spacePartitionerConfig)
				.withMaximumRegionSize(maximumRegionSize)
				.withMinimumRegionSize(minimumRegionSize)
				.withReplicationFactor(replicationFactor)
				.build();
				
		return new CreateDistributionGroupRequest(sequenceNumber, distributionGroup, configuration);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_CREATE_DISTRIBUTION_GROUP;
	}

	public String getDistributionGroup() {
		return distributionGroup;
	}
	
	public DistributionGroupConfiguration getDistributionGroupConfiguration() {
		return distributionGroupConfiguration;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((distributionGroup == null) ? 0 : distributionGroup.hashCode());
		result = prime * result
				+ ((distributionGroupConfiguration == null) ? 0 : distributionGroupConfiguration.hashCode());
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
		if (distributionGroupConfiguration == null) {
			if (other.distributionGroupConfiguration != null)
				return false;
		} else if (!distributionGroupConfiguration.equals(other.distributionGroupConfiguration))
			return false;
		return true;
	}
}
