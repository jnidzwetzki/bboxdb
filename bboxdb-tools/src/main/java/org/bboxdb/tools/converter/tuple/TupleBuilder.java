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
package org.bboxdb.tools.converter.tuple;

import java.util.concurrent.atomic.AtomicLong;

import org.bboxdb.storage.entity.Tuple;

public abstract class TupleBuilder {
	
	/**
	 * The default bbox padding
	 */
	protected double boxPadding = 0.0;
	
	/**
	 * The counter for the default key
	 */
	protected AtomicLong id = new AtomicLong(0);
	
	/**
	 * Build a tuple from the given strings
	 * @param data
	 * @return
	 */
	public String getKey(final String valueData) {
		final long oldId = id.getAndIncrement();
		return Long.toString(oldId);
	}
	
	/**
	 * Build a tuple from the given strings
	 * @param data
	 * @return
	 */
	public Tuple buildTuple(final String valueData) {
		return buildTuple(valueData, null);
	}
	
	/**
	 * Build a tuple from the given strings
	 * @param data
	 * @return
	 */
	public abstract Tuple buildTuple(final String valueData, final String keyData);

	/**
	 * Set the padding of the bounding box
	 * @param boxPadding
	 */
	public void setPadding(final double boxPadding) {
		this.boxPadding = boxPadding;
	}
	
	/**
	 * Get the bounding box padding
	 * @return
	 */
	public double getBoxPadding() {
		return boxPadding;
	}
}
