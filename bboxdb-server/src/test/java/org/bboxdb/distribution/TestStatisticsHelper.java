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
package org.bboxdb.distribution;

import org.bboxdb.distribution.partitioner.regionsplit.StatisticsHelper;
import org.junit.Assert;
import org.junit.Test;


public class TestStatisticsHelper {
	
	/**
	 * The tablename used in tests
	 */
	private final static String TABLENAME = "abc_123";

	@Test
	public void testAboveBelowValue() {
		StatisticsHelper.clearHistory();
		
		Assert.assertEquals(0, StatisticsHelper.isValueAboveStatistics(TABLENAME, 0));
		Assert.assertEquals(0, StatisticsHelper.isValueBelowStatistics(TABLENAME, 0));

		final int historyLength = StatisticsHelper.HISTORY_LENGTH;
		
		for(int i = 0; i < historyLength; i++) {
			StatisticsHelper.updateStatisticsHistory(TABLENAME, i);
		}
		
		Assert.assertEquals(historyLength / 2 - 1, StatisticsHelper.isValueAboveStatistics(TABLENAME, 5));
		Assert.assertEquals(historyLength / 2, StatisticsHelper.isValueBelowStatistics(TABLENAME, 5));

		StatisticsHelper.updateStatisticsHistory(TABLENAME, 11);
		Assert.assertEquals(historyLength / 2, StatisticsHelper.isValueAboveStatistics(TABLENAME, 5));
		Assert.assertEquals(historyLength / 2 - 1, StatisticsHelper.isValueBelowStatistics(TABLENAME, 5));

		StatisticsHelper.clearHistory();
		Assert.assertEquals(0, StatisticsHelper.isValueAboveStatistics(TABLENAME, 0));
		Assert.assertEquals(0, StatisticsHelper.isValueBelowStatistics(TABLENAME, 0));
	}
}
