package de.fernunihagen.dna.jkn.scalephant;

import java.io.IOException;

import org.junit.Test;

import junit.framework.Assert;
import de.fernunihagen.dna.jkn.scalephant.util.ObjectSerializer;

public class TestObjectSerializer {
	
	@Test
	public void testSerializer1() throws ClassNotFoundException, IOException {
		final String test = "This is a test";
		ObjectSerializer<String> serializer = new ObjectSerializer<String>();
		Assert.assertEquals(test, serializer.deserialize(serializer.serialize(test)));
	}
	
	@Test
	public void testSerializer2() throws ClassNotFoundException, IOException {
		final PersonEntity test = new PersonEntity("Jan", "Jensen", 30);
		ObjectSerializer<PersonEntity> serializer = new ObjectSerializer<PersonEntity>();
		Assert.assertEquals(test, serializer.deserialize(serializer.serialize(test)));
	}
	
}
