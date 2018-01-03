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
package org.bboxdb.network.client;

import java.util.concurrent.atomic.AtomicInteger;

public class SequenceNumberGenerator {

	/**
	 * The next unused free request id
	 */
	protected final AtomicInteger sequenceNumber = new AtomicInteger();
	
	public SequenceNumberGenerator() {
		sequenceNumber.set(0);
	}
	
	/**
	 * Get the next free sequence number
	 * 
	 * @return The sequence number
	 */
	public short getNextSequenceNummber() {
		return (short) sequenceNumber.getAndIncrement();
	}
	
	/**
	 * Get the curent sequence number
	 * 
	 * @return The sequence number
	 */
	public short getSequeneNumberWithoutIncrement() {
		return sequenceNumber.shortValue();
	}
}
