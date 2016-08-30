package de.fernunihagen.dna.scalephant.distribution;

import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;

class OudatedRegion {
	
	/**
	 * The distributed region which is outdated
	 */
	protected final DistributionRegion distributedRegion;
	
	/**
	 * The instance which contains the newest data
	 */
	protected final DistributedInstance newestInstance;
	
	/**
	 * The local version of the data
	 */
	protected final long localVersion;
	
	public OudatedRegion(final DistributionRegion distributedRegion, final DistributedInstance newestInstance, final long localVersion) {
		this.distributedRegion = distributedRegion;
		this.newestInstance = newestInstance;
		this.localVersion = localVersion;
	}

	@Override
	public String toString() {
		return "OudatedRegion [distributedRegion=" + distributedRegion
				+ ", newestInstance=" + newestInstance + ", localVersion="
				+ localVersion + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((distributedRegion == null) ? 0 : distributedRegion
						.hashCode());
		result = prime * result + (int) (localVersion ^ (localVersion >>> 32));
		result = prime * result
				+ ((newestInstance == null) ? 0 : newestInstance.hashCode());
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
		OudatedRegion other = (OudatedRegion) obj;
		if (distributedRegion == null) {
			if (other.distributedRegion != null)
				return false;
		} else if (!distributedRegion.equals(other.distributedRegion))
			return false;
		if (localVersion != other.localVersion)
			return false;
		if (newestInstance == null) {
			if (other.newestInstance != null)
				return false;
		} else if (!newestInstance.equals(other.newestInstance))
			return false;
		return true;
	}

	/**
	 * The distributed region
	 * @return
	 */
	public DistributionRegion getDistributedRegion() {
		return distributedRegion;
	}

	/**
	 * The newest instance for the region
	 * @return
	 */
	public DistributedInstance getNewestInstance() {
		return newestInstance;
	}

	/**
	 * The local version of the outdated region
	 * @return
	 */
	public long getLocalVersion() {
		return localVersion;
	}
	
}