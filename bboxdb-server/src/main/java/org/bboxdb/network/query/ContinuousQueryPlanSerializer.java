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
package org.bboxdb.network.query;

import org.json.JSONObject;

public class ContinuousQueryPlanSerializer {

	/**
	 * Serialize to JSON
	 * @param queryPlan
	 * @return
	 */
	public static String toJSON(final AbstractContinuousQueryPlan queryPlan) {
		final JSONObject json = new JSONObject();
		json.put("type", "query-plan");
		
		if(queryPlan instanceof ContinuousConstQuery) {
			json.put("query-type", "const-query");
		} else if(queryPlan instanceof ContinuousTableQuery) {
			json.put("query-type", "table-query");
		} else {
			throw new IllegalArgumentException("Unknown query type: " + queryPlan);
		}
		
		json.put("query-range", queryPlan.getQueryRange().toCompactString());
		
		return json.toString();
	}
	
	/**
	 * Deserialize query plan
	 * @param json
	 * @return
	 */
	public static AbstractContinuousQueryPlan fromJSON(final String json) {
		return null;
	}
	
}
