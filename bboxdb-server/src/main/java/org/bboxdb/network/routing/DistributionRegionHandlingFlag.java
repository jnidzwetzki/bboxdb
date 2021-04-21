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
package org.bboxdb.network.routing;

import java.util.EnumSet;

public enum DistributionRegionHandlingFlag {
	
	STREAMING_ONLY(1<<0);
	
    private final int statusFlagValue;
	
	DistributionRegionHandlingFlag(final int statusFlagValue) {
            this.statusFlagValue = statusFlagValue;
	}

	/**
	 * Get the status flag value
	 * @return
	 */
    public int getStatusFlagValue(){
        return statusFlagValue;
    } 
    
    /**
     * Convert a set of options into a value 
     * @param options
     * @return
     */
    public static int toValue(final EnumSet<DistributionRegionHandlingFlag> options) {
    	int result = 0;
    	
    	for(final DistributionRegionHandlingFlag option : options) {
    		result = result | option.getStatusFlagValue();
    	}
    	
    	return result;
    }
    
    /**
     * Convert a value into a set of options
     */
    public static EnumSet<DistributionRegionHandlingFlag> fromValue(final int options) {
    	final EnumSet<DistributionRegionHandlingFlag> result = EnumSet.noneOf(DistributionRegionHandlingFlag.class);
    	
    	for(final DistributionRegionHandlingFlag option : DistributionRegionHandlingFlag.values()) {
    		final int flag = option.getStatusFlagValue();
			if((options & flag) == flag) {
    			result.add(option);
    		}
    	}
    	
    	return result;
    }
}