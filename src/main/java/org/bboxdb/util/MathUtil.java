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

import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

public class MathUtil {

	/**
	 * The return value when parsing has failed and no exit is requested
	 */
	public static int RESULT_PARSE_FAILED_AND_NO_EXIT_INT = -1;

	/**
	 * The return value when parsing has failed and no exit is requested
	 */
	public static boolean RESULT_PARSE_FAILED_AND_NO_EXIT_BOOL = false;


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
		return tryParseInt(valueToParse, () -> "Unable to convert to integer: " + valueToParse, true);
	}

	/**
	 * Try to convert the given string into an integer
	 * @param valueToParse
	 * @param message
	 * @return
	 */
	public static int tryParseIntOrExit(final String valueToParse, 
			final Supplier<String> errorMessageSupplier) {

		return tryParseInt(valueToParse, errorMessageSupplier, true);
	}

	/**
	 * Try to convert the given string into an integer
	 * @param valueToParse
	 * @param errorMessageSupplier
	 * @return
	 */
	@VisibleForTesting
	public static int tryParseInt(final String valueToParse, 
			final Supplier<String> errorMessageSupplier, final boolean exitOnException) {

		try {
			final int parsedInteger = Integer.parseInt(valueToParse);
			return parsedInteger;
		} catch (NumberFormatException e) {
			System.err.println(errorMessageSupplier.get());

			if(exitOnException) {
				System.exit(-1);
			}
		}

		return RESULT_PARSE_FAILED_AND_NO_EXIT_INT;
	}


	/**
	 * Try to convert the given string into a double
	 * @param valueToParse
	 * @param message
	 * @return
	 */
	public static double tryParseDoubleOrExit(final String valueToParse) {
		return tryParseDouble(valueToParse, () -> "Unable to convert to double: " + valueToParse, true);
	}

	/**
	 * Try to convert the given string into a double
	 * @param valueToParse
	 * @param message
	 * @return
	 */
	public static double tryParseDoubleOrExit(final String valueToParse, 
			final Supplier<String> errorMessageSupplier) {

		return tryParseDouble(valueToParse, errorMessageSupplier, true);
	}

	/**
	 * Try to convert the given string into a double
	 * @param valueToParse
	 * @param message
	 * @return
	 */
	@VisibleForTesting
	public static double tryParseDouble(final String valueToParse,
			final Supplier<String> errorMessageSupplier, final boolean exitOnException) {
		try {
			final double parsedInteger = Double.parseDouble(valueToParse);
			return parsedInteger;
		} catch (NumberFormatException e) {
			System.err.println(errorMessageSupplier.get());

			if(exitOnException) {
				System.exit(-1);
			}
		}

		return RESULT_PARSE_FAILED_AND_NO_EXIT_INT;
	}


	/**
	 * Try to convert the given string into a double
	 * @param valueToParse
	 * @param message
	 * @return
	 */
	public static boolean tryParseBooleanOrExit(final String valueToParse) {
		return tryParseBooleanOrExit(valueToParse, () -> "Unable to convert to double: " + valueToParse, true);
	}

	/**
	 * Try to convert the given string into a double
	 * @param valueToParse
	 * @param message
	 * @return
	 */
	public static boolean tryParseBooleanOrExit(final String valueToParse, 
			final Supplier<String> errorMessageSupplier) {

		return tryParseBooleanOrExit(valueToParse, errorMessageSupplier, true);
	}

	/**
	 * Try to convert the given string into a double
	 * @param valueToParse
	 * @param message
	 * @return
	 */
	@VisibleForTesting
	public static boolean tryParseBooleanOrExit(final String valueToParse,
			final Supplier<String> errorMessageSupplier, final boolean exitOnException) {
		
		try {
			final boolean parsedBoolean = Boolean.parseBoolean(valueToParse);
			return parsedBoolean;
		} catch (NumberFormatException e) {
			System.err.println(errorMessageSupplier.get());

			if(exitOnException) {
				System.exit(-1);
			}
		}

		return RESULT_PARSE_FAILED_AND_NO_EXIT_BOOL;
	}
}
