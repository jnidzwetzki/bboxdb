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

import java.util.function.Supplier;

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
	 * The default error supplier
	 */
	public final static Supplier<String> DEFAULT_ERROR_SUPPLIER = () -> "Parse error";

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
		return tryParseIntOrExit(valueToParse, () -> "Unable to convert to integer: " + valueToParse);
	}

	/**
	 * Try to convert the given string into an integer
	 * @param valueToParse
	 * @param message
	 * @return
	 */
	public static int tryParseIntOrExit(final String valueToParse, 
			final Supplier<String> errorMessageSupplier) {

		try {
			return tryParseInt(valueToParse, errorMessageSupplier);
		} catch (InputParseException e) {
			System.err.println(e.getMessage());
			System.exit(-1);	
		}

		// Unreachable code
		return RESULT_PARSE_FAILED_AND_NO_EXIT_INT;
	}

	/**
	 * Try to convert the given string into an integer
	 * @param valueToParse
	 * @param errorMessageSupplier
	 * @return
	 * @throws InputParseException 
	 */
	public static int tryParseInt(final String valueToParse, 
			final Supplier<String> errorMessageSupplier) throws InputParseException {

		if(valueToParse == null) {
			throw new InputParseException(errorMessageSupplier.get());
		}

		try {
			return Integer.parseInt(valueToParse);
		} catch (NumberFormatException e) {
			throw new InputParseException(errorMessageSupplier.get());
		}
	}

	/**
	 * Try to convert the given string into a double
	 * @param valueToParse
	 * @param message
	 * @return
	 */
	public static double tryParseDoubleOrExit(final String valueToParse) {
		return tryParseDoubleOrExit(valueToParse, () -> "Unable to convert to double: " + valueToParse);
	}

	/**
	 * Try to convert the given string into a double
	 * @param valueToParse
	 * @param message
	 * @return
	 */
	public static double tryParseDoubleOrExit(final String valueToParse, 
			final Supplier<String> errorMessageSupplier) {

		try {
			return tryParseDouble(valueToParse, errorMessageSupplier);
		} catch (InputParseException e) {
			System.err.println(e.getMessage());
			System.exit(-1);	
		}

		// Unreachable code
		return RESULT_PARSE_FAILED_AND_NO_EXIT_INT;
	}

	/**
	 * Try to convert the given string into a long
	 * @param valueToParse
	 * @param message
	 * @return
	 * @throws InputParseException 
	 */
	public static long tryParseLong(final String valueToParse,
			final Supplier<String> errorMessageSupplier) throws InputParseException {

		if(valueToParse == null) {
			throw new InputParseException(errorMessageSupplier.get());
		}

		try {
			return Long.parseLong(valueToParse);
		} catch (NumberFormatException e) {
			throw new InputParseException(errorMessageSupplier.get());
		}
	}


	/**
	 * Try to convert the given string into a long
	 * @param valueToParse
	 * @param message
	 * @return
	 */
	public static long tryParseLongOrExit(final String valueToParse) {
		return tryParseLongOrExit(valueToParse, () -> "Unable to convert to long: " + valueToParse);
	}

	/**
	 * Try to convert the given string into a long
	 * @param valueToParse
	 * @param message
	 * @return
	 */
	public static long tryParseLongOrExit(final String valueToParse, 
			final Supplier<String> errorMessageSupplier) {

		try {
			return tryParseLong(valueToParse, errorMessageSupplier);
		} catch (InputParseException e) {
			System.err.println(e.getMessage());
			System.exit(-1);	
		}

		// Unreachable code
		return RESULT_PARSE_FAILED_AND_NO_EXIT_INT;
	}

	/**
	 * Try to convert the given string into a double
	 * @param valueToParse
	 * @param message
	 * @return
	 * @throws InputParseException 
	 */
	public static double tryParseDouble(final String valueToParse,
			final Supplier<String> errorMessageSupplier) throws InputParseException {

		if(valueToParse == null) {
			throw new InputParseException(errorMessageSupplier.get());
		}

		try {
			return Double.parseDouble(valueToParse);
		} catch (NumberFormatException e) {
			throw new InputParseException(errorMessageSupplier.get());
		}
	}

	/**
	 * Try to convert the given string into a double
	 * @param valueToParse
	 * @param message
	 * @return
	 */
	public static boolean tryParseBooleanOrExit(final String valueToParse) {
		return tryParseBooleanOrExit(valueToParse, () -> "Unable to convert to double: " + valueToParse);
	}

	/**
	 * Try to convert the given string into a double
	 * @param valueToParse
	 * @param message
	 * @return
	 */
	public static boolean tryParseBooleanOrExit(final String valueToParse, 
			final Supplier<String> errorMessageSupplier) {

		try {
			return tryParseBoolean(valueToParse, errorMessageSupplier);
		} catch (InputParseException e) {
			System.err.println(e.getMessage());
			System.exit(-1);	
		}

		// Unreachable code
		return RESULT_PARSE_FAILED_AND_NO_EXIT_BOOL;
	}

	/**
	 * Try to convert the given string into a double
	 * @param valueToParse
	 * @param message
	 * @return
	 * @throws InputParseException 
	 */
	public static boolean tryParseBoolean(final String valueToParse,
			final Supplier<String> errorMessageSupplier) throws InputParseException {

		// Boolean.parseBoolean(valueToParse) does not thow an exception on invalid input

		if(valueToParse == null) {
			throw new InputParseException(errorMessageSupplier.get());
		}

		if(valueToParse.toLowerCase().equals("true") || valueToParse.equals("1")) {
			return true;
		}

		if(valueToParse.toLowerCase().equals("false") || valueToParse.equals("0")) {
			return false;
		}

		throw new InputParseException(errorMessageSupplier.get());
	}
}
