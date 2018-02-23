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
	
	private final static double DELTA = 0.0001;

	@Test
	public void testStatististicsHistory() {
		StatisticsHelper.clearHistory();
		
		Assert.assertEquals(0, StatisticsHelper.getAverageStatistics(TABLENAME), DELTA);
		Assert.assertFalse(StatisticsHelper.isEnoughHistoryDataAvailable(TABLENAME));

		final int historyLength = StatisticsHelper.HISTORY_LENGTH;
		
		for(int i = 0; i < historyLength; i++) {
			StatisticsHelper.updateStatisticsHistory(TABLENAME, i);
		}
		
		Assert.assertEquals(2, StatisticsHelper.getAverageStatistics(TABLENAME), DELTA);
		Assert.assertTrue(StatisticsHelper.isEnoughHistoryDataAvailable(TABLENAME));

		StatisticsHelper.updateStatisticsHistory(TABLENAME, 11);
		Assert.assertEquals(4.2, StatisticsHelper.getAverageStatistics(TABLENAME), DELTA);
		Assert.assertTrue(StatisticsHelper.isEnoughHistoryDataAvailable(TABLENAME));

		StatisticsHelper.clearHistory();
		Assert.assertEquals(0, StatisticsHelper.getAverageStatistics(TABLENAME), DELTA);
		Assert.assertFalse(StatisticsHelper.isEnoughHistoryDataAvailable(TABLENAME));
	}
}
