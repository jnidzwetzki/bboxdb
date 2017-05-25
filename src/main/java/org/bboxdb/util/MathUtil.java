/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.util;

public class MathUtil {

	/**
	 * Round the given number of fractions
	 * @param value
	 * @param frac
	 * @return
	 */
    public static double round(final double value, final int frac) {
        final double pow = Math.pow(10.0, frac);
        
		return Math.round(pow * value) / pow;
    }
    
    /**
     * Try to convert the given string into an integer
     * @param valueToParse
     * @param message
     * @return
     */
    public static int tryParseIntOrExit(final String valueToParse) {
    	try {
			final int parsedInteger = Integer.parseInt(valueToParse);
			return parsedInteger;
		} catch (NumberFormatException e) {
			System.err.println("Unable to convert to integer: " + valueToParse);
			System.exit(-1);
		}
    	
    	// Dead code
    	return -1;
    }

}
