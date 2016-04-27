package de.fernunihagen.dna.jkn.scalephant.distribution;

public class ZookeeperException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7300654632387071778L;

	public ZookeeperException() {
		super();
	}

	public ZookeeperException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ZookeeperException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ZookeeperException(final String message) {
		super(message);
	}

	public ZookeeperException(final Throwable cause) {
		super(cause);
	}
}
