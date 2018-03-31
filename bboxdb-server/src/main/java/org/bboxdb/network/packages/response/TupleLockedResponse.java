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
package org.bboxdb.network.packages.response;

import java.nio.ByteBuffer;

import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packages.PackageEncodeException;

public class TupleLockedResponse extends AbstractBodyResponse {
	
	public TupleLockedResponse(final short sequenceNumber) {
		super(sequenceNumber, "");
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.RESPONSE_TYPE_TUPLE_LOCK_SUCCESS;
	}

	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PackageEncodeException 
	 */
	public static TupleLockedResponse decodePackage(final ByteBuffer encodedPackage) throws PackageEncodeException {
		decodeMessage(encodedPackage,  NetworkConst.RESPONSE_TYPE_TUPLE_LOCK_SUCCESS);
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);
		
		return new TupleLockedResponse(requestId);
	}

}
