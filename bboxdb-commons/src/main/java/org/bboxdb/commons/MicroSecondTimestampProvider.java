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

public class MicroSecondTimestampProvider {

	/**
	 * The last currentTimeMillis
	 */
	private static long lastTimestampMillis = -1;
	
	/**
	 * The counter for this millisecond
	 */
	private static int counter = 0;
	
	/**
	 * Get a faked micro seconds timestamp. Millisecond collisions are avoided
	 * by adding a faked micro seconds counter to the timestamp
	 * @return 
	 */
	public synchronized static long getNewTimestamp() {
		
		if(counter >= 999) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				// Ignore exception
			}
		}
		
		final long currentMillis = System.currentTimeMillis();
		
		if(currentMillis != lastTimestampMillis) {
			counter = 0;
			lastTimestampMillis = currentMillis;
		}
		
		final long resultValue = currentMillis * 1000 + counter;
		
		counter++;
		
		return resultValue;
	}
}
