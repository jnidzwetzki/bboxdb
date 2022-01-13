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
package org.bboxdb.network.packets.request.helper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.misc.Const;
import org.bboxdb.query.filter.UserDefinedFilterDefinition;

public class RequestEncoderHelper {

	/**
	 * Encode the list of given UDFs into a byte array
	 * @param udfs
	 * @return
	 * @throws IOException 
	 */
	public static byte[] encodeUDFs(final List<UserDefinedFilterDefinition> udfs) throws IOException {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		final ByteBuffer encodedTupleAmount = DataEncoderHelper.intToByteBuffer(udfs.size());
		baos.write(encodedTupleAmount.array());
		
		for(final UserDefinedFilterDefinition udf : udfs) {
			final String name = udf.getUserDefinedFilterClass();
			final String value = udf.getUserDefinedFilterValue();
			
			final byte[] nameBytes = name.getBytes(Const.DEFAULT_CHARSET);
			final byte[] valueBytes = value.getBytes(Const.DEFAULT_CHARSET);

			final ByteBuffer nameLength = DataEncoderHelper.intToByteBuffer(nameBytes.length);
			baos.write(nameLength.array());
			baos.write(nameBytes);
			
			final ByteBuffer valueLength = DataEncoderHelper.intToByteBuffer(valueBytes.length);
			baos.write(valueLength.array());
			baos.write(valueBytes);
		}
		
		baos.close();
		return baos.toByteArray();
	}
	
	/**
	 * Decode the list of UFDs from a byte array
	 * @param bb
	 * @return
	 */
	public static List<UserDefinedFilterDefinition> decodeUDFs(final ByteBuffer bb) {
		
		final int numberOfUDF = bb.getInt();
		
		final List<UserDefinedFilterDefinition> resultList = new ArrayList<>(numberOfUDF);
		
		for(int i = 0; i < numberOfUDF; i++) {
			final int nameLength = bb.getInt();
			final byte[] nameBytes = new byte[nameLength];
			bb.get(nameBytes, 0, nameBytes.length);
			final String name = new String(nameBytes, Const.DEFAULT_CHARSET);
			
			final int valueLength = bb.getInt();
			final byte[] valueBytes = new byte[valueLength];
			bb.get(valueBytes, 0, valueBytes.length);
			final String value = new String(valueBytes, Const.DEFAULT_CHARSET);
			
			final UserDefinedFilterDefinition udf = new UserDefinedFilterDefinition(name, value);
			resultList.add(udf);
		}

		return resultList;
	}
	
}
