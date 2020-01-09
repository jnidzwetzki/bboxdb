/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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

public class StacktraceHelper {
	
	/**
	 * Print the full stack trace
	 */
	public static String getFormatedStacktrace() {
		final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
		return getFormatedStacktrace(stackTrace);
	}

	/**
	 * Get a formated stack trace
	 * @param stackTrace
	 * @return
	 */
	public static String getFormatedStacktrace(final StackTraceElement[] stackTrace) {
		final StringBuilder sb = new StringBuilder();
		
		sb.append("=======================\n");
		
		for(final StackTraceElement stackTraceElement : stackTrace) {
			sb.append(stackTraceElement.toString());
			sb.append("\n");
		}
		
		sb.append("=======================\n");

		return sb.toString();
	}
	
}
