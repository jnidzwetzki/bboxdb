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
package org.bboxdb.tools;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * The BBoxDB CLI 
 *
 */
public class CLI {
	
	static class Action {
		/**
		 * The name of the import action
		 */
		protected static final String IMPORT = "import";
		
		/**
		 * The name of the query action
		 */
		protected static final String QUERY = "query";
		
		/**
		 * The name of the create distribution group action
		 */
		protected static final String CREATE_DGROUP = "create_distribution_group";
		
		/**
		 * The name of the delete distribution group action
		 */
		protected static final String DELETE_DGROUP = "delete_distribution_group";
	
		/**
		 * All known actions
		 */
		protected List<String> ALL_ACTIONS 
			= Arrays.asList(IMPORT, QUERY, CREATE_DGROUP, DELETE_DGROUP);
	}
	
	static class Parameter {
		
		/**
		 * The zookeeper host
		 */
		protected static final String HOST = "host";
		
		/**
		 * The name of the cluster
		 */
		protected static final String CLUSTER_NAME = "cluster";
		
		/**
		 * The name of the action parameter
		 */
		protected static final String ACTION = "action";
		
		/**
		 * The name of the help parameter
		 */
		protected static final String HELP = "help";
	}
	
	/**
	 * Main * Main * Main * Main
	 */
	public static void main(final String[] args) {
		try {
			final Options options = buildOptions();
			final CommandLineParser parser = new DefaultParser();
			final CommandLine line = parser.parse(options, args);
			
			checkParameter(options, line);

		} catch (ParseException e) {
			System.err.println("Unable to parse commandline arguments: " + e);
			System.exit(-1);
		}
	}
	
	/**
	 * Check the command line for all needed parameter
	 * @param options
	 * @param line
	 */
	protected static void checkParameter(final Options options, final CommandLine line) {
		
		if(line.hasOption(Parameter.HELP)) {
			printHelpAndExit(options);
		}
		
		if(! line.hasOption(Parameter.ACTION)) {
			printHelpAndExit(options);
		}
					
		final List<String> requiredArgs = Arrays.asList(Parameter.HOST, 
				Parameter.CLUSTER_NAME, Parameter.ACTION);
		
		final boolean hasAllParameter = requiredArgs.stream().allMatch(s -> line.hasOption(s));
		
		if(! hasAllParameter) {
			printHelpAndExit(options);
		}
		
		final String action = line.getOptionValue(Parameter.ACTION);
		
		switch (action) {
		case Action.CREATE_DGROUP:
			System.out.println("Create dgroup");
			break;
			
		case Action.DELETE_DGROUP:
			System.out.println("Delete dgroup");
			break;
			
		case Action.IMPORT:
			System.out.println("Import");
			break;
			
		case Action.QUERY:
			System.out.println("Query");
			break;

		default:
			break;
		}
	}
	
	/**
	 * Build the command line options
	 * @return
	 */
	protected static Options buildOptions() {
		final Options options = new Options();
		
		// Help
		final Option help = Option.builder(Parameter.HELP)
				.desc("Show this help")
				.build();
		options.addOption(help);
		
		// Action
		final Option action = Option.builder(Parameter.ACTION)
				.hasArg()
				.argName("action")
				.desc("The CLI action to execute")
				.build();
		options.addOption(action);
		
		// Host
		final Option host = Option.builder(Parameter.HOST)
				.hasArg()
				.argName("host")
				.desc("The Zookeeper host to connect to")
				.build();
		options.addOption(host);
		
		// Cluster name
		final Option clusterName = Option.builder(Parameter.CLUSTER_NAME)
				.hasArg()
				.argName("clustername")
				.desc("The Zookeeper host to connect to")
				.build();
		options.addOption(clusterName);
		
		return options;
	}
	
	/**
	 * Print help and exit the program
	 * @param options 
	 */
	protected static void printHelpAndExit(final Options options) {
		final HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);
		formatter.printHelp("CLI", options);
		System.exit(-1);
	}

}
