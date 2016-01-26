package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import de.fernunihagen.dna.jkn.scalephant.storage.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.DeletedTuple;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.jkn.scalephant.storage.Tuple;

public class SSTableReader extends AbstractTableReader {

	public SSTableReader(final String name, final String directory, final File file) throws StorageManagerException {
		super(name, directory, file);
	}
	
	/**
	 * Scan the whole SSTable for the Tuple
	 * @param key
	 * @return the tuple or null	
	 * @throws StorageManagerException 
	 */
	public synchronized Tuple scanForTuple(final String key) throws StorageManagerException {
		logger.info("Scanning table " + tablebumber + " for " + key);

		try {
			resetPosition();
			
			while(memory.hasRemaining()) {
				final Tuple tuple = decodeTuple();

				// The keys are stored in lexicographical order. If the
				// next key of the sstable is greater then our search key,
				// then the key is not contained in this table.
				if(tuple.getKey().compareTo(key) > 0) {
					return null;
				}
				
				if(tuple.getKey().equals(key)) {
					return tuple;
				}
			}
		
		} catch (IOException e) {
			throw new StorageManagerException(e);
		}
		
		return null;
	}
	
	/**
	 * Get tuple at the given position
	 * 
	 * @param position
	 * @return The tuple
	 * @throws StorageManagerException
	 */
	public synchronized Tuple getTupleAtPosition(int position) throws StorageManagerException {
		
		try {
			// The memory was unmapped
			if(memory == null) {
				logger.warn("Read request to unmapped memory for relation: " + name);
				return null;
			}
			
			memory.position(position);
			
			return decodeTuple();
		} catch (Exception e) {
			try {
				throw new StorageManagerException("Exception while decoding Position: " + position +  " Size "  + fileChannel.size(), e);
			} catch (IOException e1) {
				throw new StorageManagerException(e);
			}
		}
	}

	/**
	 * Decode the tuple at the current reader position
	 * 
	 * @param reader
	 * @return
	 * @throws IOException
	 */
	public Tuple decodeTuple() throws IOException {
		final short keyLength = memory.getShort();
		final int boxLength = memory.getInt();
		final int dataLength = memory.getInt();
		final long timestamp = memory.getLong();

		byte[] keyBytes = new byte[keyLength];
		memory.get(keyBytes, 0, keyBytes.length);
		
		byte[] boxBytes = new byte[boxLength];
		memory.get(boxBytes, 0, boxBytes.length);
		
		byte[] dataBytes = new byte[dataLength];
		memory.get(dataBytes, 0, dataBytes.length);				
		
		final long[] longArray = SSTableHelper.readLongArrayFromByteBuffer(boxBytes);
		final BoundingBox boundingBox = new BoundingBox(longArray);
		
		final String keyString = new String(keyBytes);
		
		if(Arrays.equals(dataBytes,SSTableConst.DELETED_MARKER)) {
			return new DeletedTuple(keyString);
		}
		
		return new Tuple(keyString, boundingBox, dataBytes, timestamp);
	}
	
	/**
	 * Decode only the key of the tuple
	 * @return
	 * @throws IOException 
	 */
	public synchronized String decodeOnlyKeyFromTupleAtPosition(int position) throws IOException {
		
		memory.position(position);
		
		final short keyLength = memory.getShort();

		final int remainingTupleSize = SSTableHelper.INT_BYTES  // BBOX-Length
				+ SSTableHelper.INT_BYTES 						// Data-Length
				+ SSTableHelper.LONG_BYTES;						// Timestamp
		
		memory.position(memory.position() + remainingTupleSize);
		
		byte[] keyBytes = new byte[keyLength];
		memory.get(keyBytes, 0, keyBytes.length);
		
		return new String(keyBytes);
	}
	
	/**
	 * Convert to string
	 */
	@Override
	public String toString() {
		return "SSTableReader [tablebumber=" + tablebumber + ", name=" + name
				+ ", directory=" + directory + "]";
	}
}
