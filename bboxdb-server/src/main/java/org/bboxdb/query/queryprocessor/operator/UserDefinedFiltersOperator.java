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
package org.bboxdb.query.queryprocessor.operator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.query.queryprocessor.predicate.Predicate;
import org.bboxdb.query.queryprocessor.predicate.PredicateJoinedTupleFilterIterator;
import org.bboxdb.query.queryprocessor.predicate.UserDefinedFiltersPredicate;
import org.bboxdb.storage.entity.MultiTuple;

public class UserDefinedFiltersOperator implements Operator {

	/**
	 * The operator
	 */
	private final Operator parentOperator;

	/**
	 * The user defined filter operator
	 */
	private final List<UserDefinedFilterDefinition> udfs;

	public UserDefinedFiltersOperator(final List<UserDefinedFilterDefinition> udfs, final Operator parentOperator) {
		this.udfs = udfs;
		this.parentOperator = parentOperator;
	}

	@Override
	public Iterator<MultiTuple> iterator() {
		final Predicate predicate = new UserDefinedFiltersPredicate(udfs);
		return new PredicateJoinedTupleFilterIterator(parentOperator.iterator(), predicate);
	}

	@Override
	public void close() throws IOException {
		parentOperator.close();
	}
}
