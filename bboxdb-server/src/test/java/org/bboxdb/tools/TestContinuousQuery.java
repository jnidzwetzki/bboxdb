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
package org.bboxdb.tools;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.network.query.ContinuousConstQuery;
import org.bboxdb.network.query.AbstractContinuousQueryPlan;
import org.bboxdb.network.query.ContinuousQueryPlanSerializer;
import org.junit.Test;

public class TestContinuousQuery {

	@Test(timeout=60_000)
	public void testQuery1() {
		final AbstractContinuousQueryPlan continuousQueryPlan = new ContinuousConstQuery("abc", null, 
				Hyperrectangle.FULL_SPACE, new Hyperrectangle(12d, 13d, 14d, 15d), false);
		
		final String serializedQueryPlan = ContinuousQueryPlanSerializer.toJSON(continuousQueryPlan);
		
		System.out.println(serializedQueryPlan);
	}
	
}
