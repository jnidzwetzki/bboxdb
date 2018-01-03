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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.bboxdb.util.DataEncoderHelper;
import org.junit.Assert;
import org.junit.Test;

public class TestDataEncoderHelper {

	@Test
	public void testDoubleArray() {
		final double[] arrayEmpty = {};
		final double[] arrayData = {3d, 5.3d, 9d, 1d, -1, -3};
		
		final ByteBuffer emptyBytes = DataEncoderHelper.doubleArrayToByteBuffer(arrayEmpty);
		final double[] arrayEmptyResult = DataEncoderHelper.readDoubleArrayFromByte(emptyBytes.array());
		Assert.assertArrayEquals(arrayEmpty, arrayEmptyResult, 0.0);
		
		final ByteBuffer arrayBytes = DataEncoderHelper.doubleArrayToByteBuffer(arrayData);
		final double[] arrayBytesResult = DataEncoderHelper.readDoubleArrayFromByte(arrayBytes.array());
		Assert.assertArrayEquals(arrayData, arrayBytesResult, 0.0);
	}
	
	@Test
	public void testDouble() {
		final double longValue = 4534545.5345;
		final ByteBuffer byteValue = DataEncoderHelper.doubleToByteBuffer(longValue);
		final double result = DataEncoderHelper.readDoubleFromByte(byteValue.array());
		Assert.assertEquals(longValue, result, 0.0);
	}
	
	@Test
	public void testLongArray() {
		final long[] arrayEmpty = {};
		final long[] arrayData = {3, 54354, 94354, -1, -3};
		
		final ByteBuffer emptyBytes = DataEncoderHelper.longArrayToByteBuffer(arrayEmpty);
		final long[] arrayEmptyResult = DataEncoderHelper.readLongArrayFromByte(emptyBytes.array());
		Assert.assertArrayEquals(arrayEmpty, arrayEmptyResult);
		
		final ByteBuffer arrayBytes = DataEncoderHelper.longArrayToByteBuffer(arrayData);
		final long[] arrayBytesResult = DataEncoderHelper.readLongArrayFromByte(arrayBytes.array());
		Assert.assertArrayEquals(arrayData, arrayBytesResult);
	}

	@Test
	public void testLong() {
		final long longValue = 4534545;
		final ByteBuffer byteValue = DataEncoderHelper.longToByteBuffer(longValue);
		final long result = DataEncoderHelper.readLongFromByte(byteValue.array());
		Assert.assertEquals(longValue, result);
	}
	
	@Test
	public void testInt() throws IOException {
		final int intValue = 4534545;
		final ByteBuffer byteValue = DataEncoderHelper.intToByteBuffer(intValue);
		final int result = DataEncoderHelper.readIntFromByte(byteValue.array());
		Assert.assertEquals(intValue, result);
		
		final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteValue.array());
		final int streamInt = DataEncoderHelper.readIntFromStream(byteArrayInputStream);
		Assert.assertEquals(intValue, streamInt);
		
		final DataInput dis = new DataInputStream(new ByteArrayInputStream(byteValue.array()));
		final int dataInputInt = DataEncoderHelper.readIntFromDataInput(dis);
		Assert.assertEquals(intValue, dataInputInt);
	}
	
	@Test
	public void testShort() {
		final short shortValue = 11;
		final ByteBuffer byteValue = DataEncoderHelper.shortToByteBuffer(shortValue);
		final long result = DataEncoderHelper.readShortFromByte(byteValue.array());
		Assert.assertEquals(shortValue, result);
	}
	
	@Test
	public void testConstants() {
		Assert.assertTrue(DataEncoderHelper.DOUBLE_BYTES > 0);
		Assert.assertTrue(DataEncoderHelper.INT_BYTES > 0);
		Assert.assertTrue(DataEncoderHelper.LONG_BYTES > 0);
		Assert.assertTrue(DataEncoderHelper.SHORT_BYTES > 0);
	}

}
