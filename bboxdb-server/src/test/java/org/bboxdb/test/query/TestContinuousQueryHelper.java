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
package org.bboxdb.test.query;

import java.util.Arrays;
import java.util.Map;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.misc.Const;
import org.bboxdb.network.entity.TupleAndBoundingBox;
import org.bboxdb.network.query.filter.UserDefinedFilter;
import org.bboxdb.network.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.network.query.filter.UserDefinedGeoJsonSpatialFilter;
import org.bboxdb.network.query.transformation.KeyFilterTransformation;
import org.bboxdb.network.query.transformation.TupleTransformation;
import org.bboxdb.network.server.query.continuous.ContinuousQueryHelper;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.junit.Assert;
import org.junit.Test;

public class TestContinuousQueryHelper {
	
	@Test(timeout=60_000)
	public void testWatermarkTuple() {
		
		final Tuple tuple = new Tuple("1", Hyperrectangle.FULL_SPACE, "".getBytes(Const.DEFAULT_CHARSET));
		final TupleStoreName tupleStorename = new TupleStoreName("abc_table_3");
		
		final MultiTuple watermarkTuple = ContinuousQueryHelper.getWatermarkTuple(tupleStorename, tuple);
		
		Assert.assertEquals(1, watermarkTuple.getNumberOfTuples());
		Assert.assertEquals("WATERMARK_abc_table_3", watermarkTuple.getTuple(0).getKey());
		Assert.assertEquals(tuple.getVersionTimestamp(), watermarkTuple.getTuple(0).getVersionTimestamp());
	}
	
	@Test(timeout=60_000)
	public void testApplyStreamTupleTransformations() {
		final Tuple tuple = new Tuple("1", Hyperrectangle.FULL_SPACE, "".getBytes(Const.DEFAULT_CHARSET));
		final TupleTransformation tupleTransformation1 = new KeyFilterTransformation("1");
		final TupleTransformation tupleTransformation2 = new KeyFilterTransformation("2");

		final TupleAndBoundingBox result1 = ContinuousQueryHelper.applyStreamTupleTransformations(Arrays.asList(tupleTransformation1), tuple);
		Assert.assertEquals("1", result1.getTuple().getKey());
		
		final TupleAndBoundingBox result2 = ContinuousQueryHelper.applyStreamTupleTransformations(Arrays.asList(tupleTransformation1, tupleTransformation2), 
				tuple);
		Assert.assertNull(result2);
	}
	
	@Test(timeout=60_000)
	public void testGetUserDefinedFilter() {
		final UserDefinedFilterDefinition userDefinedFilterDefinition = new UserDefinedFilterDefinition(UserDefinedGeoJsonSpatialFilter.class.getCanonicalName(), "");
	
		final Map<UserDefinedFilter, byte[]> udfs = ContinuousQueryHelper.getUserDefinedFilter(Arrays.asList(userDefinedFilterDefinition));
	
		Assert.assertEquals(1, udfs.size());
		Assert.assertTrue(udfs.keySet().iterator().next() instanceof UserDefinedGeoJsonSpatialFilter);
	}
	
	@Test(timeout=60_000)
	public void testDoUserDefinedFilterMatch1() {
		final Tuple tuple = new Tuple("1", Hyperrectangle.FULL_SPACE, "{}".getBytes(Const.DEFAULT_CHARSET));
		
		final UserDefinedFilterDefinition userDefinedFilterDefinition = new UserDefinedFilterDefinition(UserDefinedGeoJsonSpatialFilter.class.getCanonicalName(), "lanes:4");
		final Map<UserDefinedFilter, byte[]> udfs = ContinuousQueryHelper.getUserDefinedFilter(Arrays.asList(userDefinedFilterDefinition));
		
		final boolean filterResult = ContinuousQueryHelper.doUserDefinedFilterMatch(tuple, udfs);
		Assert.assertFalse(filterResult);
	}
	
	@Test(timeout=60_000)
	public void testDoUserDefinedFilterMatch2() {
		final Tuple tuple = new Tuple("1", Hyperrectangle.FULL_SPACE, "{}".getBytes(Const.DEFAULT_CHARSET));

		final UserDefinedFilterDefinition userDefinedFilterDefinition = new UserDefinedFilterDefinition(UserDefinedGeoJsonSpatialFilter.class.getCanonicalName(), "lanes:4");
		final Map<UserDefinedFilter, byte[]> udfs = ContinuousQueryHelper.getUserDefinedFilter(Arrays.asList(userDefinedFilterDefinition));
		
		final boolean filterResult = ContinuousQueryHelper.doUserDefinedFilterMatch(tuple, tuple, udfs);
		Assert.assertFalse(filterResult);
	}
}
