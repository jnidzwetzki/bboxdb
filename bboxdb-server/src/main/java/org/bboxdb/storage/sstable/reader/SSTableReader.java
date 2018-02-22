/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
package org.bboxdb.storage.sstable.reader;

import java.io.File;
import java.io.IOException;

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.bboxdb.storage.util.TupleHelper;

import io.prometheus.client.Counter;

public class SSTableReader extends AbstractTableReader {
	
	/**
	 * The total read tuple keys counter
	 */
	protected final static Counter readTupleKeysTotal = Counter.build()
			.name("bboxdb_read_tuple_keys_total")
			.help("Total read tuple keys").register();
	
	/**
	 * The total read tuples counter
	 */
	protected final static Counter readTuplesTotal = Counter.build()
			.name("bboxdb_read_tuple_total")
			.help("Total read tuples").register();
	
	/**
	 * The total read tuples bytes counter
	 */
	protected final static Counter readTuplesBytes = Counter.build()
			.name("bboxdb_read_tuple_bytes")
			.help("Total read tuple bytes").register();

	public SSTableReader(final String directory, final TupleStoreName tablename, final int tablenumer) throws StorageManagerException {
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
				final Tuple tuple = TupleHelper.decodeTuple(memory);

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
	public synchronized Tuple getTupleAtPosition(final int position) throws StorageManagerException {
		
		try {
			// The memory was unmapped
			if(memory == null) {
				logger.warn("Read request to unmapped memory for relation: " + name);
				return null;
			}
			
			memory.position(position);
			
			final Tuple tuple = TupleHelper.decodeTuple(memory);
			final int newPosition = memory.position();
			final int readBytes = newPosition - position;

			readTuplesTotal.inc();
			readTuplesBytes.inc(readBytes);
			
			return tuple;
		} catch (Exception e) {
			try {
				throw new StorageManagerException("Exception while decoding Position: " + position +  " Size "  + fileChannel.size(), e);
			} catch (IOException e1) {
				throw new StorageManagerException(e);
			}
		}
	}
	
	/**
	 * Decode only the key of the tuple
	 * @return
	 * @throws IOException 
	 */
	public synchronized String decodeOnlyKeyFromTupleAtPosition(final int position) throws IOException {
		
		memory.position(position);
		
		final short keyLength = memory.getShort();

		final int sizeToSkip = DataEncoderHelper.INT_BYTES          // BBOX-Length
				+ DataEncoderHelper.INT_BYTES 						// Data-Length
				+ DataEncoderHelper.LONG_BYTES						// Version Timestamp
				+ DataEncoderHelper.LONG_BYTES;						// Received Timetamp		
		
		memory.position(memory.position() + sizeToSkip);
		
		final byte[] keyBytes = new byte[keyLength];
		memory.get(keyBytes, 0, keyBytes.length);
		
		readTupleKeysTotal.inc();
		
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
		final String filename = SSTableHelper.getSSTableFilename(directory, name, tablebumber);
		return new File(filename);
	}

	@Override
	protected byte[] getMagicBytes() {
		return SSTableConst.MAGIC_BYTES;
	}
}
