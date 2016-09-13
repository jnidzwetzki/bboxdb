package de.fernunihagen.dna.scalephant.network.packages;

public abstract class NetworkResponsePackage implements NetworkPackage {
	
	/**
	 * The sequence number of the package
	 */
	protected final short sequenceNumber;
	

	public NetworkResponsePackage(final short sequenceNumber) {
		super();
		this.sequenceNumber = sequenceNumber;
	}

	/**
	 * Encode the package
	 * @return 
	 * @throws PackageEncodeError 
	 */
	public abstract byte[] getByteArray() throws PackageEncodeError;

	/**
	 * Get the sequence number of the package
	 * @return
	 */
	public short getSequenceNumber() {
		return sequenceNumber;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + sequenceNumber;
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
		NetworkResponsePackage other = (NetworkResponsePackage) obj;
		if (sequenceNumber != other.sequenceNumber)
			return false;
		return true;
	}
	
}
