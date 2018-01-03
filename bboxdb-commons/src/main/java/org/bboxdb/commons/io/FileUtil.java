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
package org.bboxdb.commons.io;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class FileUtil {
	
	/**
	 * Delete the dir on exit
	 */
	public static void deleteDirOnExit(final Path dirToDelete) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
	        @Override
	        public void run() {
	        	deleteRecursive(dirToDelete);
	        }
	    });
	}
	
	/**
	 * Delete the files recursive
	 * @param dirToDelete
	 * @return 
	 */
	public static boolean deleteRecursive(final Path dirToDelete) {
		final Path directory = dirToDelete.toAbsolutePath();
		
		if(! directory.toFile().exists()) {
			return false;
		}
		
    	try {
			Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
			   @Override
			   public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			       Files.delete(file);
			       return FileVisitResult.CONTINUE;
			   }

			   @Override
			   public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
			       Files.delete(dir);
			       return FileVisitResult.CONTINUE;
			   }
			});
		} catch (IOException e) {
			System.err.println("Got Exception: " + e);
		}
    	
    	return true;
	}
}
