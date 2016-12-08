/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *    
 *******************************************************************************/
package de.fernunihagen.dna.scalephant.storage.sstable.reader;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.DeletedTuple;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableConst;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableHelper;
import de.fernunihagen.dna.scalephant.util.DataEncoderHelper;

public class SSTableReader extends AbstractTableReader {

	public SSTableReader(final String directory, final SSTableName tablename, final int tablenumer) throws StorageManagerException {
		super(directory, tablename, tablenumer);
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

		final byte[] keyBytes = new byte[keyLength];
		memory.get(keyBytes, 0, keyBytes.length);
		
		final byte[] boxBytes = new byte[boxLength];
		memory.get(boxBytes, 0, boxBytes.length);
		
		final byte[] dataBytes = new byte[dataLength];
		memory.get(dataBytes, 0, dataBytes.length);				
		
		final BoundingBox boundingBox = BoundingBox.fromByteArray(boxBytes);
		
		final String keyString = new String(keyBytes);
		
		if(Arrays.equals(dataBytes,SSTableConst.DELETED_MARKER)) {
			return new DeletedTuple(keyString, timestamp);
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

		final int remainingTupleSize = DataEncoderHelper.INT_BYTES  // BBOX-Length
				+ DataEncoderHelper.INT_BYTES 						// Data-Length
				+ DataEncoderHelper.LONG_BYTES;						// Timestamp
		
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

	@Override
	public String getServicename() {
		return "SSTable reader";
	}

	/**
	 * Construct the filename to read
	 */
	@Override
	protected File constructFileToRead() {
		final String filename = SSTableHelper.getSSTableFilename(directory, name.getFullname(), tablebumber);
		return new File(filename);
	}
}
