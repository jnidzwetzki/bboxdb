package de.fernunihagen.dna.jkn.scalephant.distribution.placement;

public class ResourceAllocationException extends Exception {

	private static final long serialVersionUID = 7306928862288145302L;

	public ResourceAllocationException() {

	}

	public ResourceAllocationException(final String message) {
		super(message);
	}

	public ResourceAllocationException(final Throwable cause) {
		super(cause);
	}

	public ResourceAllocationException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ResourceAllocationException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
