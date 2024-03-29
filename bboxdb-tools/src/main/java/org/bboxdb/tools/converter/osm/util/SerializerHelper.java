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
package org.bboxdb.tools.converter.osm.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SerializerHelper<Type> {

	/**
	 * Convert object to byte array
	 * @return
	 * @throws IOException 
	 */
	public byte[] toByteArray(final Type object) throws IOException {
		final ByteArrayOutputStream bOutputStream = new ByteArrayOutputStream();
		final ObjectOutputStream objectOutputStream = new ObjectOutputStream(bOutputStream);

		objectOutputStream.writeObject(object);
		
		bOutputStream.close();
		return bOutputStream.toByteArray();
	}
	
	/**
	 * Construct object from byte array
	 * @param bytes
	 * @return
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	@SuppressWarnings("unchecked")
	public Type loadFromByteArray(final byte[] bytes) throws IOException, ClassNotFoundException {
		final ByteArrayInputStream bInputStream = new ByteArrayInputStream(bytes);
		final ObjectInputStream oInputStream = new ObjectInputStream(bInputStream);
		
		return (Type) oInputStream.readObject();
	}

	
}
