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

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.storage.util.TupleHelper;

public class WriteAheadLogWriter implements Closeable {

	/**
	 * The file writer
	 */
	private BufferedOutputStream os;

	/**
	 * The file
	 */
	private final File file;

	public WriteAheadLogWriter(final File basedir, final long memtableNumber) throws IOException {

		this.file = WriteAheadLogManager.getFileForWal(basedir, memtableNumber);

		if(file.exists()) {
			throw new RuntimeException("File " + file + " does already exist");
		}

		this.os = new BufferedOutputStream(new FileOutputStream(file));

		os.write(SSTableConst.MAGIC_BYTES_WAL);
	}

	/**
	 * Add a tuple to the WAL
	 * @param tuple
	 * @throws IOException
	 */
	public void addTuple(final Tuple tuple) throws StorageManagerException {
		try {
			assert (os != null) : "Writer can not be null";
			TupleHelper.writeTupleToStream(tuple, os);
			os.flush();
		} catch (IOException e) {
			throw new StorageManagerException(e);
		}
	}

	/**
	 * Close the WAL writer
	 */
	@Override
	public void close() throws IOException {
		if(os != null) {
			os.close();
			os = null;
		}
	}

	/**
	 * Get the written file
	 * @return
	 */
	public File getFile() {
		return file;
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
