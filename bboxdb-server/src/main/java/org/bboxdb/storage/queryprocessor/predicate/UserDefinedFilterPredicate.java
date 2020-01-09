/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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

import org.bboxdb.network.query.filter.UserDefinedFilter;
import org.bboxdb.storage.entity.Tuple;

public class UserDefinedFilterPredicate implements Predicate {

	/**
	 * The user defined filter
	 */
	private UserDefinedFilter userDefinedFilter;

	/**
	 * The custom filter date
	 */
	private byte[] customFilterData;

	public UserDefinedFilterPredicate(final UserDefinedFilter userDefinedFilter,
			final byte[] customFilterData) {

		this.userDefinedFilter = userDefinedFilter;
		this.customFilterData = customFilterData;
	}

	@Override
	public boolean matches(final Tuple tuple) {
		return userDefinedFilter.filterTuple(tuple, customFilterData);
	}

	@Override
	public String toString() {
		return "UserDefinedFilterPredicate [userDefinedFilter=" + userDefinedFilter + "]";
	}

}
