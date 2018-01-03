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
package org.bboxdb.commons;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ObjectSerializer<T> {

	/**
	 * Serialize the given object into a byte array
	 * @param object
	 * @return
	 * @throws IOException
	 */
	public byte[] serialize(T object) throws IOException {
		
		try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final ObjectOutputStream out = new ObjectOutputStream(baos)
		) {
			out.writeObject(object);
			return baos.toByteArray();
		}
	}
	
	/**
	 * Deserialize a given byte array 
	 * @param stream
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	public T deserialize(final byte[] stream) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream(stream);
		
		try(final ObjectInput input = new ObjectInputStream(bis)) {
			return (T) input.readObject();
		}
	}
}