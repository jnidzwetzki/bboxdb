/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.storage;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;

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
		invalidNames.add("0_df_def_");
		invalidNames.add("0_df_def_a");
		invalidNames.add("0_df_def__");
		invalidNames.add("_____");

		for(final String invalidTablename : invalidNames) {
			final SSTableName tablename = new SSTableName(invalidTablename);
			Assert.assertFalse(tablename.isValid());
			Assert.assertEquals(SSTableName.INVALID_GROUP, tablename.getGroup());
			Assert.assertEquals(SSTableName.INVALID_DIMENSION, tablename.getDimension());
			Assert.assertEquals(SSTableName.INVALID_TABLENAME, tablename.getTablename());
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
		validNames.add("122_12def_table21_1");
		validNames.add("122_12def_table21_4711");
		
		for(final String validTablename : validNames) {
			final SSTableName tablename = new SSTableName(validTablename);
			Assert.assertTrue(tablename.isValid());
			Assert.assertNotNull(tablename.getGroup());
			Assert.assertNotNull(tablename.getDimension());
			Assert.assertNotNull(tablename.getTablename());
		}
	}
}
