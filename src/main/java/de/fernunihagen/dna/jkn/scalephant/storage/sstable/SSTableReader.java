package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;
import java.io.IOException;

import de.fernunihagen.dna.jkn.scalephant.storage.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.jkn.scalephant.storage.Tuple;

public class SSTableReader extends AbstractTableReader {
	
	/**
	 * Buffer for the tuple decoder
	 */
	protected byte[] keyLengthBytes = new byte[SSTableHelper.SHORT_BYTES];
	protected byte[] boxLengthBytes = new byte[SSTableHelper.INT_BYTES];
	protected byte[] dataLengthBytes = new byte[SSTableHelper.INT_BYTES];
	protected byte[] timestampBytes = new byte[SSTableHelper.LONG_BYTES];
	
	public SSTableReader(final String name, final String directory, final File file) throws StorageManagerException {
		super(name, directory, file);
	}
	
	/**
	 * Scan the whole SSTable for the Tuple
	 * @param key
	 * @return the tuple or null	
	 * @throws StorageManagerException 
	 */
	public Tuple scanForTuple(final String key) throws StorageManagerException {
		logger.info("Search in table: " + tablebumber + " for " + key);

		try {
			resetFileReaderPosition();
			
			while(reader.available() > 0) {
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
	public Tuple getTupleAtPosition(int position) throws StorageManagerException {
		try {
			fileInputStream.getChannel().position(position);
			createNewReaderBuffer();
			
			final Tuple tuple = decodeTuple();
			
			return tuple;
		} catch (IOException e) {
			throw new StorageManagerException(e);
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
		reader.read(keyLengthBytes, 0, keyLengthBytes.length);
		reader.read(boxLengthBytes, 0, boxLengthBytes.length);
		reader.read(dataLengthBytes, 0, dataLengthBytes.length);
		reader.read(timestampBytes, 0, timestampBytes.length);
		
		final short keyLength = SSTableHelper.readShortFromByteBuffer(keyLengthBytes);
		final int boxLength = SSTableHelper.readIntFromByteBuffer(boxLengthBytes);
		final int dataLength = SSTableHelper.readIntFromByteBuffer(dataLengthBytes);
		final long timestamp = SSTableHelper.readLongFromByteBuffer(timestampBytes);
		
		byte[] keyBytes = new byte[keyLength];
		reader.read(keyBytes, 0, keyBytes.length);
		
		byte[] boxBytes = new byte[boxLength];
		reader.read(boxBytes, 0, boxBytes.length);
		
		byte[] dataBytes = new byte[dataLength];
		reader.read(dataBytes, 0, dataBytes.length);				
		
		final long[] longArray = SSTableHelper.readLongArrayFromByteBuffer(boxBytes);
		final BoundingBox boundingBox = new BoundingBox(longArray);
		
		final String keyString = new String(keyBytes);
		
		return new Tuple(keyString, boundingBox, dataBytes, timestamp);
	}
	
	/**
	 * Decode only the key of the tuple
	 * @return
	 * @throws IOException 
	 */
	public String decodeOnlyKeyFromTuple() throws IOException {
		reader.read(keyLengthBytes, 0, keyLengthBytes.length);
		reader.skip(boxLengthBytes.length);
		reader.skip(dataLengthBytes.length);
		reader.skip(timestampBytes.length);
		
		final short keyLength = SSTableHelper.readShortFromByteBuffer(keyLengthBytes);

		byte[] keyBytes = new byte[keyLength];
		reader.read(keyBytes, 0, keyBytes.length);
		
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
