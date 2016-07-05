package de.fernunihagen.dna.jkn.scalephant.distribution.membership;

import java.net.InetSocketAddress;

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
	 * The version number of the instance
	 */
	protected String version = UNKOWN_VERSION;
	
	
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
			ip = parts[0];
			port = portInterger;
		} catch(NumberFormatException e) {
			throw new IllegalArgumentException("Unable to parse: " + parts[1], e);
		}
	}

	public DistributedInstance(final String localIp, final Integer localPort, final String version) {
		this.ip = localIp;
		this.port = localPort;
		this.version = version;
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
		return true;
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
		return "DistributedInstance [ip=" + ip + ", port=" + port + ", version=" + version + "]";
	}
	
	/**
	 * Get the inet socket address from the instance
	 * @return
	 */
	public InetSocketAddress getInetSocketAddress() {
		return new InetSocketAddress(ip, port);
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
	
}
