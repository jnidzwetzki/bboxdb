/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
package org.bboxdb.experiments;

import java.io.File;

import org.bboxdb.commons.math.Hyperrectangle;

public class DetermineBoundingBox {

	/***
	 * Main * Main * Main * Main
	 * @param args
	 */
	public static void main(final String[] args) {
		
		if(args.length != 2) {
			System.err.println("Usage: <Class> <Filename> <Format>");
			System.exit(-1);
		}
		
		final String filename = args[0];
		final String inputformat = args[1];
		
		final File file = new File(filename);
		
		if(! file.isFile()) {
			System.err.println("Unable to open: " + file);
			System.exit(-1);
		}
		
		System.out.println("Processing file: " + filename + " with format " + inputformat);
		final Hyperrectangle boundingBox = ExperimentHelper.determineBoundingBox(filename, inputformat);
		System.out.println("Bounding box is:" + boundingBox);
	}
}
