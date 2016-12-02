/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.distribution.membership;

import java.net.InetSocketAddress;

import de.fernunihagen.dna.scalephant.distribution.membership.event.DistributedInstanceState;

public class DistributedInstance implements Comparable<DistributedInstance> {
	
	public final static String UNKOWN_VERSION = "unknown";
	
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
	protected String version = UNKOWN_VERSION;
	
	/**
	 * The state of the instance
	 */
	protected DistributedInstanceState state = DistributedInstanceState.UNKNOWN;

	public DistributedInstance(final String connectionString, final String version, final DistributedInstanceState state) {
		this(connectionString, version);
		this.state = state;
	}
	
	public DistributedInstance(final String connectionString, final String version) {
		this(connectionString);
		this.version = version;
	}
	
	public DistributedInstance(final String connectionString) {
		final String[] parts = connectionString.split(":");
		
		if(parts.length != 2) {
			throw new IllegalArgumentException("Unable to parse:" + connectionString);
		}
		
		try {
			final Integer portInterger = Integer.parseInt(parts[1]);
			this.ip = parts[0];
			this.port = portInterger;
			this.socketAddress = new InetSocketAddress(ip, port);
		} catch(NumberFormatException e) {
			throw new IllegalArgumentException("Unable to parse: " + parts[1], e);
		}
	}

	public DistributedInstance(final String localIp, final Integer localPort, final String version) {
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
		DistributedInstance other = (DistributedInstance) obj;
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
	public boolean socketAddressEquals(final DistributedInstance instance) {
		if(instance == null) {
			return false;
		}
		
		return instance.getInetSocketAddress().equals(getInetSocketAddress());
	}

	public String toGUIString() {
		if(version == UNKOWN_VERSION) {
			return "DistributedInstance [ip=" + ip + ", port=" + port + "]";
		} else {
			return "DistributedInstance [ip=" + ip + ", port=" + port + ", version=" + version + "]";
		}
	}

	@Override
	public String toString() {
		return "DistributedInstance [ip=" + ip + ", port=" + port
				+ ", version=" + version + ", state=" + state + "]";
	}

	/**
	 * Get the inet socket address from the instance
	 * @return
	 */
	public InetSocketAddress getInetSocketAddress() {
		return socketAddress;
	}
	
	/**
	 * Convert the data back into a string
	 * @return
	 */
	public String getStringValue() {
		return ip + ":" + port;
	}

	@Override
	public int compareTo(final DistributedInstance otherInstance) {
		return getStringValue().compareTo(otherInstance.getStringValue());
	}
	
	/**
	 * Get the state of the instance
	 * @return
	 */
	public DistributedInstanceState getState() {
		return state;
	}
	
	/**
	 * Set the state of the instance
	 * @param state
	 */
	public void setState(final DistributedInstanceState state) {
		this.state = state;
	}
	
}
