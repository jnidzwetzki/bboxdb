/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.distribution.region;

import java.util.HashSet;
import java.util.Set;

public class GlobalDistributionRegionEventBus {
	
	private final Set<DistributionRegionCallback> callbacks;
	
	private final static GlobalDistributionRegionEventBus instance;
	
	static {
		instance = new GlobalDistributionRegionEventBus();
	}

	/**
	 * Get the instance
	 * @return
	 */
	public static GlobalDistributionRegionEventBus getInstance() {
		return instance;
	}
	
	/**
	 * Singleton - Private constructor
	 */
	private GlobalDistributionRegionEventBus() {
		this.callbacks = new HashSet<>();
	}
	
	/**
	 * Register a new callback
	 * @param callback
	 */
	public void registerCallback(final DistributionRegionCallback callback) {
		callbacks.add(callback);
	}
	
	/**
	 * Remove a callback
	 * @param callback
	 * @return 
	 */
	public boolean removeCallback(final DistributionRegionCallback callback) {
		return callbacks.remove(callback);
	}
	
	/**
	 * Run a callback
	 * @param region
	 * @param event
	 */
	public void runCallback(final DistributionRegion region, final DistributionRegionEvent event) {
		callbacks.forEach(c -> c.regionChanged(event, region));
	}
	
}
