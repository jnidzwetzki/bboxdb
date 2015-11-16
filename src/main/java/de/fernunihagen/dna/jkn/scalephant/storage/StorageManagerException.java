package de.fernunihagen.dna.jkn.scalephant.storage;

public class StorageManagerException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1839787108363978798L;

	public StorageManagerException() {
		super();
	}

	public StorageManagerException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public StorageManagerException(String message, Throwable cause) {
		super(message, cause);
	}

	public StorageManagerException(String message) {
		super(message);
	}

	public StorageManagerException(Throwable cause) {
		super(cause);
	}
	
}
