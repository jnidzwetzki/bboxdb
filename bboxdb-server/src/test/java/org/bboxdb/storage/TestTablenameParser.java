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
package org.bboxdb.storage;

import java.util.ArrayList;
import java.util.List;

import org.bboxdb.storage.entity.TupleStoreName;
import org.junit.Assert;
import org.junit.Test;

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
			final TupleStoreName tablename = new TupleStoreName(invalidTablename);
			Assert.assertFalse(tablename.isValid());
			Assert.assertEquals(TupleStoreName.INVALID_GROUP, tablename.getDistributionGroup());
			Assert.assertEquals(TupleStoreName.INVALID_TABLENAME, tablename.getTablename());
		}
		
	}
	
	/**
	 * Test the parsing of valid tablenames
	 */
	@Test
	public void testTablenameParserValid() {
		final List<String> validNames = new ArrayList<String>();
		validNames.add("abc_def");
		validNames.add("34_34");
		validNames.add("def_34");
		validNames.add("def_table21");
		validNames.add("12def_table21");
		validNames.add("12def_table21_1");
		validNames.add("12def_table21_4711");
		
		for(final String validTablename : validNames) {
			final TupleStoreName tablename = new TupleStoreName(validTablename);
			Assert.assertTrue(tablename.isValid());
			Assert.assertNotNull(tablename.getDistributionGroup());
			Assert.assertNotNull(tablename.getTablename());
		}
	}
}
