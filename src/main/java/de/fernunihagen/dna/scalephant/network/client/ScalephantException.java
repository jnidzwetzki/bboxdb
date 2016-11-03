package de.fernunihagen.dna.scalephant.network.client;

public class ScalephantException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5173204916779805788L;

	public ScalephantException() {

	}

	public ScalephantException(final String message) {
		super(message);
	}

	public ScalephantException(final Throwable cause) {
		super(cause);
	}

	public ScalephantException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ScalephantException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
