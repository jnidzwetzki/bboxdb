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
package org.bboxdb.networkproxy.test;

import java.io.IOException;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.networkproxy.misc.TupleStringSerializer;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.junit.Assert;
import org.junit.Test;

public class TupleStringSerializerTest {

	@Test(timeout=60000)
	public void testTuple1() throws IOException {
		final Tuple tuple = new Tuple("abc", Hyperrectangle.FULL_SPACE, "".getBytes());
		final String serirializedTuple = TupleStringSerializer.tupleToProxyString(tuple);
		final Tuple unserializedTuple = TupleStringSerializer.proxyStringToTuple(serirializedTuple);
		Assert.assertEquals(tuple, unserializedTuple);
	}
	
	@Test(timeout=60000)
	public void testTuple2() throws IOException {
		final Tuple tuple = new Tuple("key2", new Hyperrectangle(1.0, 2.0, -1.0, 5.0), "".getBytes());
		final String serirializedTuple = TupleStringSerializer.tupleToProxyString(tuple);
		final Tuple unserializedTuple = TupleStringSerializer.proxyStringToTuple(serirializedTuple);
		Assert.assertEquals(tuple, unserializedTuple);
	}
	
	@Test(timeout=60000)
	public void testTuple3() throws IOException {
		final Tuple tuple = new Tuple("key3", new Hyperrectangle(1.0, 2.0, -1.0, 5.0), "abcdef".getBytes());
		final String serirializedTuple = TupleStringSerializer.tupleToProxyString(tuple);
		final Tuple unserializedTuple = TupleStringSerializer.proxyStringToTuple(serirializedTuple);
		Assert.assertEquals(tuple, unserializedTuple);
	}
	@Test(timeout=60000)
	public void testTuple4() throws IOException {
		final Tuple tuple = new Tuple("key4", new Hyperrectangle(1.0, 2.0, -1.0, 5.0), "abcdef".getBytes(), 12345);
		final String serirializedTuple = TupleStringSerializer.tupleToProxyString(tuple);
		final Tuple unserializedTuple = TupleStringSerializer.proxyStringToTuple(serirializedTuple);
		Assert.assertEquals(tuple, unserializedTuple);
	}
	
	@Test(timeout=60000)
	public void testDeletedTuple1() throws IOException {
		final Tuple tuple = new DeletedTuple("key2");
		final String serirializedTuple = TupleStringSerializer.tupleToProxyString(tuple);
		final Tuple unserializedTuple = TupleStringSerializer.proxyStringToTuple(serirializedTuple);
		Assert.assertEquals(tuple, unserializedTuple);
	}
	
	@Test(timeout=60000)
	public void testDeletedTuple2() throws IOException {
		final Tuple tuple = new DeletedTuple("key2", 123456);
		final String serirializedTuple = TupleStringSerializer.tupleToProxyString(tuple);
		final Tuple unserializedTuple = TupleStringSerializer.proxyStringToTuple(serirializedTuple);
		Assert.assertEquals(tuple, unserializedTuple);
	}
	
	@Test(timeout=60000, expected=IOException.class)
	public void testInvalidTuple1() throws IOException {
		TupleStringSerializer.proxyStringToTuple(null);
	}
	
	@Test(timeout=60000, expected=IOException.class)
	public void testInvalidTuple2() throws IOException {
		TupleStringSerializer.proxyStringToTuple("");
	}
	
	@Test(timeout=60000, expected=IOException.class)
	public void testInvalidTuple3() throws IOException {
		TupleStringSerializer.proxyStringToTuple("abc");
	}
	
	@Test(timeout=60000, expected=IOException.class)
	public void testInvalidTuple4() throws IOException {
		TupleStringSerializer.proxyStringToTuple("5 abc");
	}
}
