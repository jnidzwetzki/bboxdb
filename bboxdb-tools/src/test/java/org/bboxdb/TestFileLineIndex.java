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
package org.bboxdb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.bboxdb.tools.FileLineIndex;
import org.junit.Assert;
import org.junit.Test;

public class TestFileLineIndex {

	/**
	 * Test indexing of a non existing file
	 * @throws IOException
	 */
	@Test(expected=IOException.class)
	public void testNonExistingFile() throws IOException {
		final File tempFile = File.createTempFile("temp",".txt");
		
		tempFile.delete();
		Assert.assertFalse(tempFile.exists());
		
		try(final FileLineIndex fli = new FileLineIndex(tempFile.getAbsolutePath())) {
			fli.indexFile();
		}
	}
	
	/**
	 * Test reading of a non indexed file
	 * @throws IOException 
	 */
	@Test(expected=IllegalArgumentException.class)
	public void readUnindexed() throws IOException {
		final File tempFile = File.createTempFile("temp",".txt");
		tempFile.deleteOnExit();
		
		try(final FileLineIndex fli = new FileLineIndex(tempFile.getAbsolutePath())) {
			fli.locateLine(12);
		}
	}

	/**
	 * Test file indexing
	 * @throws IOException 
	 */
	@Test
	public void testIndexEmpty1() throws IOException {
		final File tempFile = File.createTempFile("temp",".txt");
		tempFile.deleteOnExit();
		
		final BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile));
		bw.write("");
		bw.close();
		
		try (final FileLineIndex fli = new FileLineIndex(tempFile.getAbsolutePath())) {
			fli.indexFile();
			
			Assert.assertEquals(1, fli.getIndexedLines());
			
			final long pos1 = fli.locateLine(1);
			Assert.assertEquals(0, pos1);
		}
	}
	
	/**
	 * Test file indexing - Invalid line
	 * @throws IOException 
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testIndexEmpty2() throws IOException {
		final File tempFile = File.createTempFile("temp",".txt");
		tempFile.deleteOnExit();
		
		final BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile));
		bw.write("");
		bw.close();
		
		try (final FileLineIndex fli = new FileLineIndex(tempFile.getAbsolutePath())) {
			fli.indexFile();	
			fli.locateLine(2);
		}
	}

	
	/**
	 * Test file indexing
	 * @throws IOException 
	 */
	@Test
	public void testIndex() throws IOException {
		final String line1 = "LINE1";
		final String line2 = "LINE2";
		final String line3 = "LINE 3";
		final String line4 = "This is file 4";
		final String line5 = "LINE 5";

		final StringBuilder sb = new StringBuilder();
		sb.append(line1);
		sb.append("\n");
		sb.append(line2);
		sb.append("\n");
		sb.append(line3);
		sb.append("\n");
		sb.append(line4);
		sb.append("\n");
		sb.append(line5);
		
		final File tempFile = File.createTempFile("temp",".txt");
		tempFile.deleteOnExit();
		
		final BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile));
		bw.write(sb.toString());
		bw.close();
		
		try(
				final FileLineIndex fli = new FileLineIndex(tempFile.getAbsolutePath());
				final RandomAccessFile file = new RandomAccessFile(tempFile, "r");
			) {
			Assert.assertEquals(0, fli.getIndexedLines());
			fli.indexFile();
			Assert.assertEquals(5, fli.getIndexedLines());

			readLineFomIndex(file, fli, 1, line1);
			readLineFomIndex(file, fli, 2, line2);
			readLineFomIndex(file, fli, 3, line3);
			readLineFomIndex(file, fli, 4, line4);
			readLineFomIndex(file, fli, 5, line5);
		}
	}
	
	/**
	 * Read a line from the index
	 * @param pos
	 * @param expected
	 * @throws IOException 
	 */
	protected void readLineFomIndex(final RandomAccessFile file, final FileLineIndex fli, 
			final long lineNumber, final String expected) throws IOException {
		
		final long pos = fli.locateLine(lineNumber);
		file.seek(pos);
		final String line = file.readLine();
		Assert.assertEquals(expected, line);
	}
}

