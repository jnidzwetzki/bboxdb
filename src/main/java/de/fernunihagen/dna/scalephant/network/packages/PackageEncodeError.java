package de.fernunihagen.dna.scalephant.network.packages;

public class PackageEncodeError extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6883469153500992081L;

	public PackageEncodeError() {

	}

	public PackageEncodeError(final String message) {
		super(message);
	}

	public PackageEncodeError(final Throwable cause) {
		super(cause);
	}

	public PackageEncodeError(final String message, final Throwable cause) {
		super(message, cause);
	}

	public PackageEncodeError(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
