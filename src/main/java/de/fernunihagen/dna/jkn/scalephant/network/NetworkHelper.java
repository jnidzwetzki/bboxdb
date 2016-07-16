package de.fernunihagen.dna.jkn.scalephant.network;

import java.io.IOException;
import java.io.InputStream;

public class NetworkHelper {
	
	/**
	 * Read exactly n bytes into the buffer
	 * @param buffer
	 * @param bytesToRead
	 * @throws IOException 
	 */
	public static void readExactlyBytes(final InputStream inputStream, 
			final byte[] buffer, final int offset, final int bytesToRead) throws IOException {
				
		if(buffer.length < bytesToRead) {
			throw new IllegalArgumentException("Unable to read " + bytesToRead + " bytes into a buffer with size " + buffer.length);
		}
		
		int totalReadBytes = 0;
		
		while(totalReadBytes < bytesToRead) {
			int bytesRead = inputStream.read(buffer, offset + totalReadBytes, (bytesToRead - totalReadBytes));
			
			if(bytesRead <= 0) {
				throw new IOException("Return code on read operation: " + bytesRead);
			}
			
			totalReadBytes = totalReadBytes + bytesRead;
		}	
	}
	
	/**
	 * The exactly n bytes in the input stream
	 * @param inputStream
	 * @param bytesToSkip
	 * @throws IOException
	 */
	public void skipBytesExcactly(final InputStream inputStream, final int bytesToSkip) throws IOException {
		int totalSkipBytes = 0;
		
		while(totalSkipBytes < bytesToSkip) {
			int skippedBytes = (int) inputStream.skip(bytesToSkip);
			totalSkipBytes = totalSkipBytes + skippedBytes;
		}
	}

}
