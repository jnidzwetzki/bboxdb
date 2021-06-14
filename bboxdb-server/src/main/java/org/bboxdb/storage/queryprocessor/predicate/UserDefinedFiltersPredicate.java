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
package org.bboxdb.storage.queryprocessor.predicate;

import java.util.List;

import org.bboxdb.network.query.filter.UserDefinedFilter;
import org.bboxdb.network.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserDefinedFiltersPredicate implements Predicate {

	/**
	 * The user defined filter
	 */
	private final List<UserDefinedFilterDefinition> udfs;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(UserDefinedFiltersPredicate.class);


	public UserDefinedFiltersPredicate(final List<UserDefinedFilterDefinition> udfs) {
		this.udfs = udfs;
	}

	@Override
	public boolean matches(final Tuple tuple) {
		
		try {
			for(final UserDefinedFilterDefinition udf : udfs) {
				final Class<?> userDefinedFilterClass = Class.forName(udf.getUserDefinedFilterClass());
				final UserDefinedFilter userDefinedFilter = (UserDefinedFilter) userDefinedFilterClass.newInstance();
				final boolean matches = userDefinedFilter.filterTuple(tuple, udf.getUserDefinedFilterValue().getBytes());
			
				if(! matches) {
					return false;
				}
			}
		} catch (Exception e) {
			logger.error("Got exception while performing UDF", e);
			return false;
		}
		
		
		return true;
	}

	@Override
	public String toString() {
		return "UserDefinedFilterPredicate [userDefinedFilter=" + udfs + "]";
	}

}
