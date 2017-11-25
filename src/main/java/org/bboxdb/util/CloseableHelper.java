package org.bboxdb.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

public class CloseableHelper {

	/**
	 * A consumer that prints the exception on stderr
	 */
	public static final Consumer<Exception> PRINT_EXCEPTION_ON_STDERR = 
				(e) -> System.err.println("Exception while closing: " + e);
	
	/**
	 * Close the closeable without throwing an exception or logging
	 * @param closeable
	 */
	public static void closeWithoutException(final Closeable closeable) {
		closeWithoutException(closeable, null);
	}
	
	/**
	 * Close the closeable without throwing an exception or logging
	 * @param closeable
	 */
	public static void closeWithoutException(final Closeable closeable, final Consumer<Exception> consumer) {
		
		/**
		 * Wrap the Closeable interface into a AutoCloseable interface and resuse the
		 * AutoCloseable close logic
		 */
		final AutoCloseable closeableWrapper = new AutoCloseable() {	
			@Override
			public void close() throws IOException {
				closeable.close();
			}
		};
		
		closeWithoutException(closeableWrapper, consumer);
	}
	
	/**
	 * Close the closeable without throwing an exception or logging
	 * @param closeable
	 */
	public static void closeWithoutException(final AutoCloseable closeable) {
		closeWithoutException(closeable, null);
	}
	
	/**
	 * Close the closeable without throwing an exception or logging
	 * @param closeable
	 */
	public static void closeWithoutException(final AutoCloseable closeable, final Consumer<Exception> consumer) {
		if(closeable == null) {
			return;
		}
		
		try {
			closeable.close();
		} catch (Exception e) {
			if(consumer != null) {
				consumer.accept(e);
			}
		}
	}

}
