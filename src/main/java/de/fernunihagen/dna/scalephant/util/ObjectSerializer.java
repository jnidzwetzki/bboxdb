package de.fernunihagen.dna.scalephant.util;

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