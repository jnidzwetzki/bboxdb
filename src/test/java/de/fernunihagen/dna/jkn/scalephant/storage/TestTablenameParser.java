package de.fernunihagen.dna.jkn.scalephant.storage;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.util.Tablename;

public class TestTablenameParser {
	
	/**
	 * Test the parsing of invalid tablenames
	 */
	@Test
	public void testTablenameParserInvalid() {
		final List<String> invalidNames = new ArrayList<String>();
		invalidNames.add("");
		invalidNames.add(null);
		invalidNames.add("abc");
		invalidNames.add("abcd_abcd_abce");
		invalidNames.add("__");
		invalidNames.add("3__");
		invalidNames.add("3_abc_");
		invalidNames.add("3_abc_def_gef");
		invalidNames.add("__def");
		invalidNames.add("abc__def");
		invalidNames.add("-0_df_def");
		invalidNames.add("1-0_df_def");
		invalidNames.add("-1_df_def");
		invalidNames.add("0_df_def");

		for(final String invalidTablename : invalidNames) {
			final Tablename tablename = new Tablename(invalidTablename);
			Assert.assertFalse(tablename.isValid());
			Assert.assertEquals(Tablename.INVALID_GROUP, tablename.getGroup());
			Assert.assertEquals(Tablename.INVALID_DIMENSION, tablename.getDimension());
			Assert.assertEquals(Tablename.INVALID_IDENTIFIER, tablename.getIdentifier());
		}
		
	}
	
	/**
	 * Test the parsing of valid tablenames
	 */
	@Test
	public void testTablenameParserValid() {
		final List<String> validNames = new ArrayList<String>();
		validNames.add("3_abc_def");
		validNames.add("15_34_34");
		validNames.add("122_def_34");
		validNames.add("122_def_table21");
		validNames.add("122_12def_table21");
		
		for(final String validTablename : validNames) {
			final Tablename tablename = new Tablename(validTablename);
			Assert.assertTrue(tablename.isValid());
			Assert.assertNotNull(tablename.getGroup());
			Assert.assertNotNull(tablename.getDimension());
			Assert.assertNotNull(tablename.getIdentifier());
		}
	}
}
