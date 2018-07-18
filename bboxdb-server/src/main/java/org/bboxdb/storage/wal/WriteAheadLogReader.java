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
package org.bboxdb.storage.wal;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.storage.util.TupleHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

public class WriteAheadLogReader implements Closeable, Iterable<Tuple> {

	/**
	 * The input stream
	 */
	private BufferedInputStream inputStream;

	/**
	 * The file
	 */
	private final File file;

	/**
	 * The Logger
	 */
	private static final Logger logger = LoggerFactory.getLogger(WriteAheadLogReader.class);

	/**
	 * The tuple iterator
	 */
	private final class TupleIterator implements Iterator<Tuple> {

		/**
		 * The next tuple
		 */
		private Tuple nextTuple = null;

		@Override
		public boolean hasNext() {

			// We have already fetched a tuple
			if(nextTuple != null) {
				return true;
			}

			try {
				final int availableBytes = inputStream.available();

				if(availableBytes == 0) {
					return false;
				}

				nextTuple = TupleHelper.decodeTuple(inputStream);

				return true;
			} catch (IOException e) {
				logger.error("Got IO exception", e);
				return false;
			}
		}

		@Override
		public Tuple next() {

			if(nextTuple == null) {
				throw new RuntimeException("next() called but nextTupe is null,"
						+ " do you forget to call hasNext()?");
			}

			final Tuple tupleToReturn = nextTuple;
			nextTuple = null;
			return tupleToReturn;
		}
	}

	public WriteAheadLogReader(final File basedir, final int memtableNumber) throws IOException, StorageManagerException {
		this(WriteAheadLogManager.getFileForWal(basedir, memtableNumber));
	}

	public WriteAheadLogReader(final File file) throws IOException, StorageManagerException {

		if(! file.exists()) {
			throw new IOException("File " + file + " does not exist");
		}

		this.file = file;
		this.inputStream = new BufferedInputStream(new FileInputStream(file));

		// Validate file - read the magic from the beginning
		final byte[] expectedMagic = SSTableConst.MAGIC_BYTES_WAL;
		final byte[] magicBytes = new byte[expectedMagic.length];

		ByteStreams.readFully(inputStream, magicBytes, 0, expectedMagic.length);

		if(! Arrays.equals(magicBytes, expectedMagic)) {
			throw new StorageManagerException("File " + file + " does not contain the magic bytes");
		}
	}

	/**
	 * Close the WAL reader
	 */
	@Override
	public void close() throws IOException {
		if(inputStream != null) {
			inputStream.close();
			inputStream = null;
		}
	}

	@Override
	public Iterator<Tuple> iterator() {
		return new TupleIterator();
	}

	/**
	 * Delete the base file
	 * @throws IOException
	 */
	public void deleteFile() throws IOException {
		close();

		if(file.exists()) {
			final boolean deleteResult = file.delete();

			if(! deleteResult) {
				throw new IOException("Unable to delete: " + file);
			}
		}
	}
}
