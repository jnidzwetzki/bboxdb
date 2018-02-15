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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class SequenceNumberGenerator {

	/**
	 * The next unused free request id
	 */
	protected final AtomicInteger sequenceNumber = new AtomicInteger();
	
	/**
	 * The used numbers
	 */
	protected final Set<Short> usedNumbers = new HashSet<>();
	
	/**
	 * The amount of tried to find an empty sequence number
	 */
	private final static int MAX_TRIES = 10000;
	
	public SequenceNumberGenerator() {
		sequenceNumber.set(0);
	}
	
	/**
	 * Get the next free sequence number
	 * 
	 * @return The sequence number
	 */
	public synchronized short getNextSequenceNummber() {
		short nextNumber = (short) sequenceNumber.getAndIncrement();
		short numberTry = 1;
				
		// Check if sequence number is unused
		while(usedNumbers.contains(nextNumber)) {
			
			if(numberTry > MAX_TRIES) {
				throw new IllegalArgumentException("Unable to get sequence number with tries: " + MAX_TRIES);
			}
			
			nextNumber = (short) sequenceNumber.getAndIncrement();
			numberTry++;
		}
		
		usedNumbers.add(nextNumber);
		return nextNumber;
	}
	
	/**
	 * Get the curent sequence number
	 * 
	 * @return The sequence number
	 */
	public short getSequeneNumberWithoutIncrement() {
		return sequenceNumber.shortValue();
	}
	
	/**
	 * Release the given sequence number
	 * @param sequenceNumber
	 * @return
	 */
	public synchronized boolean releaseNumber(final short sequenceNumber) {
		return usedNumbers.remove(sequenceNumber);
	}
}
