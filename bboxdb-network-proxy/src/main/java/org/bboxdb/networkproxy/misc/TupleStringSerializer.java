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
package org.bboxdb.networkproxy.misc;

import java.io.IOException;
import java.util.Optional;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.util.TupleHelper;

public class TupleStringSerializer {
	
	/**
	 * Convert the tuple into a string
	 * @param tuple
	 * @param sb
	 * @return
	 */
	public static String tupleToProxyString(final Tuple tuple) {
		final StringBuilder sb = new StringBuilder();
		
		sb.append(tuple.getKey().length());
		sb.append(" ");
		sb.append(new String(tuple.getKey()));
		sb.append(" ");
		sb.append(tuple.getBoundingBox().toCompactString());
		sb.append(" ");
		sb.append(tuple.getDataBytes().length);
		sb.append(" ");
		sb.append(new String(tuple.getDataBytes()));
		sb.append(" ");
		sb.append(tuple.getVersionTimestamp());
		
		return sb.toString();
	}
	
	/**
	 * Convert the proxy string back to a tuple
	 * @param string
	 * @return
	 */
	public static Tuple proxyStringToTuple(final String string) throws IOException {
		
		try {
			if(string == null) {
				throw new IOException("Unable to handle a null string");
			}
						
			// Key
			final int keyLengthEndPos = safeIndexOf(string, 0);
			
			final String keyLengthString = string.substring(0, keyLengthEndPos);
			final Optional<Integer> keyLengthOptional = MathUtil.tryParseInt(keyLengthString);
			
			if(! keyLengthOptional.isPresent()) {
				throw new IOException("Unable to parse key length: " + keyLengthString + ".");
			}
			
			final int keyStartPos = keyLengthEndPos + 1;
			final int keyEndPos = keyLengthEndPos + keyLengthOptional.get() + 1;
			final String key = string.substring(keyStartPos, keyEndPos);
			
			// Bounding box
			final int bboxStartPos = keyEndPos + 1;
			final int bboxEndPos = safeIndexOf(string, bboxStartPos);
			final String bboxString = string.substring(bboxStartPos, bboxEndPos);
			final Hyperrectangle bbox = Hyperrectangle.fromString(bboxString);
			
			// Value
			final int valueLengthStartPos = bboxEndPos + 1;
			final int valueLengthEndPos = safeIndexOf(string, valueLengthStartPos);
			final String valueLengthString = string.substring(valueLengthStartPos, valueLengthEndPos);

			final Optional<Integer> valueLengthOptional = MathUtil.tryParseInt(valueLengthString);
			
			if(! valueLengthOptional.isPresent()) {
				throw new IOException("Unable to parse value length: " + valueLengthString);
			}
			
			final int valueStartPos = valueLengthEndPos + 1;
			final int valueEndPos = valueStartPos + valueLengthOptional.get();
			final byte[] value = string.substring(valueStartPos, valueEndPos).getBytes();
			
			// Version timestamp
			final int timeStampStart = valueEndPos + 1;
			final int timeStampEnd = string.length();
			final String versionString = string.substring(timeStampStart, timeStampEnd);
			
			final Optional<Long> versionStringOptional = MathUtil.tryParseLong(versionString);

			if(! versionStringOptional.isPresent()) {
				throw new IOException("Unable to parse version: " + versionString);
			}
			
			final long timestamp = versionStringOptional.get();

			System.out.println(bbox);
			System.out.println(Hyperrectangle.FULL_SPACE);
			System.out.println(Hyperrectangle.FULL_SPACE == bbox);

			System.out.println(value);
			
			if(TupleHelper.isDeletedTuple(bbox, value)) {
				return new DeletedTuple(key, timestamp);
			} else {
				return new Tuple(key, bbox, value, timestamp);
			}
			
		} catch (IndexOutOfBoundsException e) {
			throw new IOException(e);
		}
	}

	/**
	 * Index of method with -1 check
	 * @param string
	 * @param startIndex
	 * @return
	 * @throws IOException
	 */
	private static int safeIndexOf(final String string, final int startIndex) throws IOException {
		
		final int keyLengthEndPos = string.indexOf(" ", startIndex);
		
		if(keyLengthEndPos == -1) {
			throw new IOException("Unable to parse: " + string);
		}
		
		return keyLengthEndPos;
	}
}
