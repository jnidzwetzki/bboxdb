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
package org.bboxdb.tools.cli;

import org.apache.commons.cli.CommandLine;

public class CLIHelper {

	/**
	 * Return the CLI parameter value or the default value 
	 * if the parameter is missing
	 * 
	 * @param line
	 * @param parameterName
	 * @param defaultValue
	 * @return
	 */
	public static String getParameterOrDefault(final CommandLine line, 
			final String parameterName, final String defaultValue) {
		
		return line.hasOption(parameterName) 
				? line.getOptionValue(parameterName)
				: defaultValue;
	}

}
