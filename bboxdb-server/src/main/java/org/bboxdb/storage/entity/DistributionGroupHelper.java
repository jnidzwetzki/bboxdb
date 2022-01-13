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
package org.bboxdb.storage.entity;

import org.bboxdb.commons.StringUtil;

public class DistributionGroupHelper {
	
	/**
	 * Validate the distribution group name
	 * 
	 * @param distributionGroupName
	 * @return
	 */
	public static boolean validateDistributionGroupName(final String distributionGroupName) {
		return StringUtil.countCharOccurrence(distributionGroupName, '_') == 0;
	}
}
