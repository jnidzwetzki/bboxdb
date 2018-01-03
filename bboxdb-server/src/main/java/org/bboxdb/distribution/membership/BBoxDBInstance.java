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
package org.bboxdb.distribution.membership;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;

public class BBoxDBInstance implements Comparable<BBoxDBInstance> {
	
	/**
	 * The string when the property is unknown
	 */
	public final static String UNKOWN_PROPERTY = "unknown";
	
	/**
	 * The IP address of the instance
	 */
	protected final String ip;
	
	/**
	 * The port of the instance
	 */
	protected final int port;
	
	/** 
	 * The inet socket address
	 */
	protected final InetSocketAddress socketAddress;
	
	/**
	 * The version number of the instance
	 */
	protected String version = UNKOWN_PROPERTY;
	
	/**
	 * The number of CPU cores
	 */
	protected int cpuCores = 0;
	
	/**
	 * The amount of memory
	 */
	protected long memory = 0;
	
	/**
	 * The total space on the storage locations
	 */
	protected final Map<String, Long> totalSpaceLocation = new HashMap<>();
	
	/**
	 * The free space on the storage locations
	 */
	protected final Map<String, Long> freeSpaceLocation = new HashMap<>();
	
	/**
	 * The state of the instance
	 */
	protected BBoxDBInstanceState state = BBoxDBInstanceState.UNKNOWN;

	public BBoxDBInstance(final String connectionString, final String version, final BBoxDBInstanceState state) {
		this(connectionString, state);
		this.version = version;
	}
	
	public BBoxDBInstance(final String connectionString, final BBoxDBInstanceState state) {
		this(connectionString);
		this.state = state;
	}

	public BBoxDBInstance(final String connectionString) {
		final String[] parts = connectionString.split(":");
		
		if(parts.length != 2) {
			throw new IllegalArgumentException("Unable to parse: " + connectionString);
		}
		
		try {
			this.port = MathUtil.tryParseInt(parts[1], MathUtil.DEFAULT_ERROR_SUPPLIER);
		} catch(InputParseException e) {
			throw new IllegalArgumentException(e);
		}
		
		this.ip = parts[0];
		this.socketAddress = new InetSocketAddress(ip, port);
	}

	public BBoxDBInstance(final String localIp, final Integer localPort, final String version) {
		this.ip = localIp;
		this.port = localPort;
		this.version = version;
		this.socketAddress = new InetSocketAddress(ip, port);
	}

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	public String getVersion() {
		return version;
	}
	
	public void setVersion(final String version) {
		this.version = version;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ip == null) ? 0 : ip.hashCode());
		result = prime * result + port;
		result = prime * result + ((state == null) ? 0 : state.hashCode());
		result = prime * result + ((version == null) ? 0 : version.hashCode());
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
		BBoxDBInstance other = (BBoxDBInstance) obj;
		if (ip == null) {
			if (other.ip != null)
				return false;
		} else if (!ip.equals(other.ip))
			return false;
		if (port != other.port)
			return false;
		if (state != other.state)
			return false;
		if (version == null) {
			if (other.version != null)
				return false;
		} else if (!version.equals(other.version))
			return false;
		return true;
	}
	
	/**
	 * Equals based on the socketAddress
	 * @param instance
	 * @return
	 */
	public boolean socketAddressEquals(final BBoxDBInstance instance) {
		if(instance == null) {
			return false;
		}
		
		return instance.getInetSocketAddress().equals(getInetSocketAddress());
	}

	public String toGUIString() {
		if(version == UNKOWN_PROPERTY) {
			return "DistributedInstance [ip=" + ip + ", port=" + port + "]";
		} else {
			return "DistributedInstance [ip=" + ip + ", port=" + port + ", version=" + version + "]";
		}
	}

	/**
	 * Get the inet socket address from the instance
	 * @return
	 */
	public InetSocketAddress getInetSocketAddress() {
		return socketAddress;
	}
	
	@Override
	public String toString() {
		return "DistributedInstance [ip=" + ip + ", port=" + port + ", version=" + version + ", "
				+ "cpuCores=" + cpuCores + ", memory=" + memory + ", state=" + state 
				+ ", storages=" + getNumberOfStorages() + ", freeSpace()=" + getFreeSpace() 
				+ ", totalSpace()=" + getTotalSpace() + "]";
	}

	/**
	 * Convert the data back into a string
	 * @return
	 */
	public String getStringValue() {
		final StringBuilder sb = new StringBuilder();
		sb.append(ip);
		sb.append(":");
		sb.append(port);
		return sb.toString();
	}

	@Override
	public int compareTo(final BBoxDBInstance otherInstance) {
		return getStringValue().compareTo(otherInstance.getStringValue());
	}
	
	/**
	 * Get the state of the instance
	 * @return
	 */
	public BBoxDBInstanceState getState() {
		return state;
	}
	
	/**
	 * Set the state of the instance
	 * @param state
	 */
	public void setState(final BBoxDBInstanceState state) {
		this.state = state;
	}

	/**
	 * Get the number of cpu cores
	 * @return
	 */
	public int getCpuCores() {
		return cpuCores;
	}

	/**
	 * Set the number of cpu cores
	 * @param cpuCores
	 */
	public void setCpuCores(final int cpuCores) {
		this.cpuCores = cpuCores;
	}

	/**
	 * Get the amount of memory
	 * @return
	 */
	public long getMemory() {
		return memory;
	}

	/**
	 * Set the amount of memory
	 * @param memory
	 */
	public void setMemory(final long memory) {
		this.memory = memory;
	}
	
	/**
	 * Add free space data
	 * @param location
	 * @param space
	 */
	public void addFreeSpace(final String location, final long space) {
		freeSpaceLocation.put(location, space);
	}
	
	/**
	 * Add total space data
	 * @param location
	 * @param space
	 */
	public void addTotalSpace(final String location, final long space) {
		totalSpaceLocation.put(location, space);
	}
	
	/**
	 * Get the free space data
	 * @return
	 */
	public Map<String, Long> getAllFreeSpaceLocations() {
		return freeSpaceLocation;
	}
	
	/**
	 * Get the total space data
	 * @return
	 */
	public Map<String, Long> getAllTotalSpaceLocations() {
		return totalSpaceLocation;
	}
	
	/** 
	 * Get the number of storages
	 */
	public int getNumberOfStorages() {
		return freeSpaceLocation.size();
	}
	
	/**
	 * Get the summed free space
	 * @return
	 */
	public long getFreeSpace() {
		return freeSpaceLocation.values().stream().mapToLong(e -> e).sum();
	}
	
	/**
	 * Get the summed total space
	 * @return
	 */
	public long getTotalSpace() {
		return totalSpaceLocation.values().stream().mapToLong(e -> e).sum();
	}
}
