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
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.misc.Const;
import org.bboxdb.network.packages.request.helper.RequestEncoderHelper;
import org.bboxdb.query.filter.UserDefinedFilterDefinition;
import org.junit.Assert;
import org.junit.Test;

public class TestRequestEncoderHelper {

	@Test(timeout = 60_000)
	public void testEncode0() throws IOException {
		final List<UserDefinedFilterDefinition> udfs = new ArrayList<>();
		final byte[] bytes = RequestEncoderHelper.encodeUDFs(udfs);
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		bb.order(Const.APPLICATION_BYTE_ORDER);
		final List<UserDefinedFilterDefinition> decodedList = RequestEncoderHelper.decodeUDFs(bb);
		
		Assert.assertEquals(udfs, decodedList);
		Assert.assertEquals(0, bb.remaining());
	}
	
	@Test(timeout = 60_000)
	public void testEncode2() throws IOException {
		final List<UserDefinedFilterDefinition> udfs = new ArrayList<>();
		udfs.add(new UserDefinedFilterDefinition("abc", ""));
		
		final byte[] bytes = RequestEncoderHelper.encodeUDFs(udfs);
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		bb.order(Const.APPLICATION_BYTE_ORDER);
		final List<UserDefinedFilterDefinition> decodedList = RequestEncoderHelper.decodeUDFs(bb);
		
		Assert.assertEquals(udfs, decodedList);
		Assert.assertEquals(0, bb.remaining());
	}
	
	@Test(timeout = 60_000)
	public void testEncode3() throws IOException {
		final List<UserDefinedFilterDefinition> udfs = new ArrayList<>();
		udfs.add(new UserDefinedFilterDefinition("abc", "def"));
		
		final byte[] bytes = RequestEncoderHelper.encodeUDFs(udfs);
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		bb.order(Const.APPLICATION_BYTE_ORDER);
		final List<UserDefinedFilterDefinition> decodedList = RequestEncoderHelper.decodeUDFs(bb);
		
		Assert.assertEquals(udfs, decodedList);
		Assert.assertEquals(0, bb.remaining());
	}
	
	@Test(timeout = 60_000)
	public void testEncode4() throws IOException {
		final List<UserDefinedFilterDefinition> udfs = new ArrayList<>();
		udfs.add(new UserDefinedFilterDefinition("abc", "def"));
		udfs.add(new UserDefinedFilterDefinition("123", "532"));

		final byte[] bytes = RequestEncoderHelper.encodeUDFs(udfs);
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		bb.order(Const.APPLICATION_BYTE_ORDER);
		final List<UserDefinedFilterDefinition> decodedList = RequestEncoderHelper.decodeUDFs(bb);
		
		Assert.assertEquals(udfs, decodedList);
		Assert.assertEquals(0, bb.remaining());
	}
}
