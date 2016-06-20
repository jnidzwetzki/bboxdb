package de.fernunihagen.dna.jkn.scalephant.distribution.membership;

import java.net.InetSocketAddress;

public class DistributedInstance {
	
	/**
	 * The IP address of the instance
	 */
	protected final String ip;
	
	/**
	 * The port of the instance
	 */
	protected final int port;
	
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

	public DistributedInstance(final String localIp, final Integer localPort) {
		ip = localIp;
		port = localPort;
	}

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	@Override
	public String toString() {
		return "DistributedInstance [ip=" + ip + ", port=" + port + "]";
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
	
}
