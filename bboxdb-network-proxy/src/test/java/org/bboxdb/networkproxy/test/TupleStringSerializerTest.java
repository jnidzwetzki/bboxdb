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
package org.bboxdb.networkproxy.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.networkproxy.misc.TupleStringSerializer;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.junit.Assert;
import org.junit.Test;

public class TupleStringSerializerTest {

	@Test(timeout=60000)
	public void testTuple1() throws IOException {
		final Tuple tuple = new Tuple("abc", Hyperrectangle.FULL_SPACE, "".getBytes());
		final byte[] serirializedTuple = TupleStringSerializer.tupleToProxyBytes(tuple);
		final Tuple unserializedTuple = TupleStringSerializer.proxyBytesToTuple(serirializedTuple);
		Assert.assertEquals(tuple, unserializedTuple);
	}

	@Test(timeout=60000)
	public void testTuple2() throws IOException {
		final Tuple tuple = new Tuple("key2", new Hyperrectangle(1.0, 2.0, -1.0, 5.0), "".getBytes());
		final byte[] serirializedTuple = TupleStringSerializer.tupleToProxyBytes(tuple);
		final Tuple unserializedTuple = TupleStringSerializer.proxyBytesToTuple(serirializedTuple);
		Assert.assertEquals(tuple, unserializedTuple);
	}

	@Test(timeout=60000)
	public void testTuple3() throws IOException {
		final Tuple tuple = new Tuple("key3", new Hyperrectangle(1.0, 2.0, -1.0, 5.0), "abcdef".getBytes());
		final byte[] serirializedTuple = TupleStringSerializer.tupleToProxyBytes(tuple);
		final Tuple unserializedTuple = TupleStringSerializer.proxyBytesToTuple(serirializedTuple);
		Assert.assertEquals(tuple, unserializedTuple);
	}
	
	@Test(timeout=60000)
	public void testTuple4() throws IOException {
		final Tuple tuple = new Tuple("key4", new Hyperrectangle(1.0, 2.0, -1.0, 5.0), "abcdef".getBytes(), 12345);
		final byte[] serirializedTuple = TupleStringSerializer.tupleToProxyBytes(tuple);
		final Tuple unserializedTuple = TupleStringSerializer.proxyBytesToTuple(serirializedTuple);
		Assert.assertEquals(tuple, unserializedTuple);
	}

	@Test(timeout=60000)
	public void testDeletedTuple1() throws IOException {
		final Tuple tuple = new DeletedTuple("key2");
		final byte[] serirializedTuple = TupleStringSerializer.tupleToProxyBytes(tuple);
		final Tuple unserializedTuple = TupleStringSerializer.proxyBytesToTuple(serirializedTuple);
		Assert.assertEquals(tuple, unserializedTuple);
	}

	@Test(timeout=60000)
	public void testDeletedTuple2() throws IOException {
		final Tuple tuple = new DeletedTuple("key2", 123456);
		final byte[] serirializedTuple = TupleStringSerializer.tupleToProxyBytes(tuple);
		final Tuple unserializedTuple = TupleStringSerializer.proxyBytesToTuple(serirializedTuple);
		Assert.assertEquals(tuple, unserializedTuple);
	}

	@Test(timeout=60000, expected=IOException.class)
	public void testInvalidTuple1() throws IOException {
		TupleStringSerializer.proxyBytesToTuple(null);
	}

	@Test(timeout=60000, expected=IOException.class)
	public void testInvalidTuple2() throws IOException {
		TupleStringSerializer.proxyBytesToTuple("".getBytes());
	}

	@Test(timeout=60000, expected=IOException.class)
	public void testInvalidTuple3() throws IOException {
		TupleStringSerializer.proxyBytesToTuple("abc".getBytes());
	}

	@Test(timeout=60000, expected=IOException.class)
	public void testInvalidTuple4() throws IOException {
		TupleStringSerializer.proxyBytesToTuple("5 abc".getBytes());
	}

	@Test(timeout=60000)
	public void testTupleStream1() throws IOException {
		final Tuple tuple = new Tuple("abc", Hyperrectangle.FULL_SPACE, "".getBytes());
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		TupleStringSerializer.writeTuple(tuple, bos);
		bos.close();
		final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		final Tuple unserializedTuple = TupleStringSerializer.readTuple(bis);
		Assert.assertEquals(tuple, unserializedTuple);
	}

	@Test(timeout=60000)
	public void testTupleStream2() throws IOException {
		final Tuple tuple = new Tuple("key2", new Hyperrectangle(1.0, 2.0, -1.0, 5.0), "".getBytes());
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		TupleStringSerializer.writeTuple(tuple, bos);
		bos.close();
		final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		final Tuple unserializedTuple = TupleStringSerializer.readTuple(bis);
		Assert.assertEquals(tuple, unserializedTuple);
	}
	
	@Test(timeout=60000)
	public void testJoinedTuple0() throws IOException {
		final MultiTuple joinedTuple = new MultiTuple(Arrays.asList(), Arrays.asList());
		
		final byte[] serirializedTuple = TupleStringSerializer.joinedTupleToProxyBytes(joinedTuple);
		final MultiTuple unserializedTuple = TupleStringSerializer.proxyBytesToJoinedTuple(serirializedTuple);
		Assert.assertEquals(joinedTuple, unserializedTuple);
	}
	
	@Test(timeout=60000)
	public void testJoinedTuple1() throws IOException {
		final Tuple tuple1 = new Tuple("abc", Hyperrectangle.FULL_SPACE, "".getBytes());
		final MultiTuple joinedTuple = new MultiTuple(Arrays.asList(tuple1), Arrays.asList("abc"));
		
		final byte[] serirializedTuple = TupleStringSerializer.joinedTupleToProxyBytes(joinedTuple);
		final MultiTuple unserializedTuple = TupleStringSerializer.proxyBytesToJoinedTuple(serirializedTuple);
		Assert.assertEquals(joinedTuple, unserializedTuple);
	}
	
	@Test(timeout=60000)
	public void testJoinedTuple2() throws IOException {
		final Tuple tuple1 = new Tuple("abc", Hyperrectangle.FULL_SPACE, "".getBytes());
		final Tuple tuple2 = new Tuple("abc", Hyperrectangle.FULL_SPACE, "def".getBytes());
		final MultiTuple joinedTuple = new MultiTuple(Arrays.asList(tuple1, tuple2), Arrays.asList("abc", "def"));
		
		final byte[] serirializedTuple = TupleStringSerializer.joinedTupleToProxyBytes(joinedTuple);
		final MultiTuple unserializedTuple = TupleStringSerializer.proxyBytesToJoinedTuple(serirializedTuple);
		Assert.assertEquals(joinedTuple, unserializedTuple);
	}
	
	@Test(timeout=60000)
	public void testJoinedTupleStream1() throws IOException {
		final Tuple tuple1 = new Tuple("abc", Hyperrectangle.FULL_SPACE, "".getBytes());
		final Tuple tuple2 = new Tuple("abc", Hyperrectangle.FULL_SPACE, "def".getBytes());
		final MultiTuple joinedTuple = new MultiTuple(Arrays.asList(tuple1, tuple2), Arrays.asList("abc", "def"));
		
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		TupleStringSerializer.writeJoinedTuple(joinedTuple, bos);
		bos.close();
		final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		final MultiTuple unserializedTuple = TupleStringSerializer.readJoinedTuple(bis);
		
		Assert.assertEquals(joinedTuple, unserializedTuple);
	}

	
}
