/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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

import com.google.common.base.Strings;

public class IsotoneStringMapper {

	/**
	 * The amount of chars that are considered for mapping
	 */
	public final static int DEFAULT_PREFIX_LENGTH = 3;
	
	/**
	 * Perform a isotone mapping with the default prefix length
	 * @param element
	 * @return
	 */
	public static double mapToDouble(final String element) {
		return mapToInt(element, DEFAULT_PREFIX_LENGTH);
	}

	/**
	 * Perform a isotone mapping from a string into a double. It implements
	 * a conversion based on the positional numeral system.
	 * 
	 * The algorithm takes the first 'prefixLength' chars from the string
	 * and performs a homeomorph mapping from the string into a double value. 
	 * 
	 * Elements that are larger than the prefix are ignored in the mapping 
	 * and the same mapping values are produced.
	 * 
	 * Example with prefix length = 3;
	 * 
	 * Value   | Mapping
	 * ===================
	 * ABCD    | 45453
	 * ABCE    | 45453
	 * ABCAAAA | 45453
	 * 
	 * @param element
	 * @return
	 */
	public static int mapToInt(final String element, final int prefixLength) {
		
		final String mapString = element.substring(0, Math.min(prefixLength, element.length()));
		
		final String prefixedMapString = Strings.padEnd(mapString, prefixLength, '\u0000');
		
		int result = 0;
				
		for(int i = 0; i < prefixedMapString.length(); i++) {
			final char theChar = prefixedMapString.charAt(i);
			final int posValue = Character.getNumericValue(theChar);
			final int exponent = prefixLength - i - 1;
			result += posValue * Math.pow(256, exponent);
		}
		
		return result;
	}
	
}
