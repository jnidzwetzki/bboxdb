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
			final byte[] buffer, final int bytesToRead) throws IOException {
		
		int offset = 0;
		
		if(buffer.length < bytesToRead) {
			throw new IllegalArgumentException("Unable to read " + bytesToRead + " into a buffer with size " + buffer.length);
		}
		
		while(offset < bytesToRead) {
			int bytesRead = inputStream.read(buffer, offset, (bytesToRead - offset));
			
			if(bytesRead <= 0) {
				throw new IOException("Return code on read operation: " + bytesRead);
			}
			
			offset = offset + bytesRead;
		}
		
	}

}
