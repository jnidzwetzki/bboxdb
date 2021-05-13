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
package org.bboxdb.test.network;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.network.packages.NetworkTupleEncoderDecoder;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleAndTable;
import org.bboxdb.storage.entity.WatermarkTuple;
import org.junit.Assert;
import org.junit.Test;

public class TestNetworkTupleEncoderDecoder {

	@Test(timeout = 60_000)
	public void testTupleEncodeDecode() throws IOException {
		final Tuple tuple1 = new Tuple("abc", new Hyperrectangle(1d, 2d), "".getBytes());
		testEncoding(tuple1);
		
		final Tuple tuple2 = new Tuple("abc", Hyperrectangle.FULL_SPACE, "".getBytes());
		testEncoding(tuple2);
	}
	
	@Test(timeout = 60_000)
	public void testDeletedTupleEncodeDecode() throws IOException {
		final Tuple tuple1 = new DeletedTuple("abc");
		testEncoding(tuple1);
	}

	@Test(timeout = 60_000)
	public void testWatermarkTupleEncodeDecode() throws IOException {
		final Tuple tuple1 = new WatermarkTuple();
		testEncoding(tuple1);
		
		final Tuple tuple2 = new WatermarkTuple(System.currentTimeMillis());
		testEncoding(tuple2);
		
		final Tuple tuple3 = new WatermarkTuple("abc",System.currentTimeMillis());
		testEncoding(tuple3);
	}
	
	/**
	 * Test the tuple encoding
	 * @param tuple
	 * @throws IOException
	 */
	private void testEncoding(final Tuple tuple) throws IOException {
		final String tableName = "mygroup_abc";
		final byte[] bytes = NetworkTupleEncoderDecoder.encode(tuple, tableName);
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		
		final TupleAndTable decodedData = NetworkTupleEncoderDecoder.decode(bb);
		
		Assert.assertEquals(tuple, decodedData.getTuple());
	}
}
