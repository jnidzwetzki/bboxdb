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
package org.bboxdb.commons;

import java.text.DecimalFormat;

public class FileSizeHelper {

	// Algorithm is based on: https://stackoverflow.com/questions/3263892/format-file-size-as-mb-gb-etc
	public static String readableFileSize(final long size) {
		if(size < 0) {
			throw new IllegalArgumentException("Negative size: " + size);
		}
		
	    final String[] units = new String[] { "B", "KB", "MB", "GB", "TB" };
	    
	    final int devider = (int) (Math.log10(size)/Math.log10(1024));
	    return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, devider)) + " " + units[devider];
	}
	
}
