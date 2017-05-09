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
package org.bboxdb.tools.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class SyntheticDataGenerator implements Runnable {

	static class Parameter {
		/**
		 * The dimenstion of the data
		 */
		public final static String DIMENSION = "dimension";

		/**
		 * The number of tuples to produce
		 */
		public final static String AMOUNT = "amount";

		/**
		 * The size per tuple
		 */
		public final static String SIZE = "size";

		/**
		 * The output file
		 */
		public final static String OUTPUTFILE = "outputfile";
		
		/**
		 * Print help
		 */
		public final static String HELP = "help";
	}

	/**
	 * The list with used random chars
	 */
	protected static char[] symbols;

	/**
	 * The parsed command line
	 */
	protected final CommandLine line;

	/**
	 * The random generator
	 */
	protected final Random random = new Random();

	static {
		StringBuilder tmp = new StringBuilder();

		for (char ch = '0'; ch <= '9'; ch++) {
			tmp.append(ch);
		}

		for (char ch = 'a'; ch <= 'z'; ch++) {
			tmp.append(ch);
		}

		symbols = tmp.toString().toCharArray();
	}

	public SyntheticDataGenerator(final CommandLine line) {
		this.line = line;
	}

	/**
	 * Build the command line options
	 * 
	 * @return
	 */
	protected static Options buildOptions() {
		final Options options = new Options();

		// Help
		final Option help = Option.builder(Parameter.HELP)
				.desc("Show this help")
				.build();
		options.addOption(help);

		// Dimension
		final Option dimension = Option.builder(Parameter.DIMENSION)
				.hasArg()
				.argName("dimension")
				.desc("The dimension of the bounding box")
				.build();
		options.addOption(dimension);

		// Amount
		final Option amount = Option
				.builder(Parameter.AMOUNT)
				.hasArg()
				.argName("amount")
				.desc("The amount of tuples to produce")
				.build();
		options.addOption(amount);

		// Size
		final Option size = Option.builder(Parameter.SIZE)
				.hasArg()
				.argName("size")
				.desc("The size in byte per tuple")
				.build();
		options.addOption(size);
		
		// Output file
		final Option outputfile = Option.builder(Parameter.OUTPUTFILE)
				.hasArg()
				.argName("file")
				.desc("The outputfile")
				.build();
		options.addOption(outputfile);

		return options;
	}

	/**
	 * Print help and exit the program
	 * 
	 * @param options
	 */
	protected static void printHelpAndExit() {

		final Options options = buildOptions();

		final String header = "Synthetic data generator\n\n";

		final String footer = "\nPlease report issues at https://github.com/jnidzwetzki/bboxdb/issues\n";

		final HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(120);
		formatter.printHelp("SyntheticDataGenerator", header, options, footer);

		System.exit(-1);
	}

	@Override
	public void run() {
		try {
			final long amount = Long.parseLong(line.getOptionValue(Parameter.AMOUNT));
			final int size = Integer.parseInt(line.getOptionValue(Parameter.SIZE));
			final int dimension = Integer.parseInt(line.getOptionValue(Parameter.DIMENSION));
			final String outputFile = line.getOptionValue(Parameter.OUTPUTFILE);
			
			System.out.format("Generating %d lines with %d bytes and %d dimensions\n", amount, size, dimension);

			final File file = new File(outputFile);
			if(file.exists()) {
				System.err.println("File " + outputFile + " already exists, exiting");
				System.exit(-1);
			}
			
			try(final BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
				LongStream.range(0, amount).forEach(l -> generateLine(size, dimension, writer));
			} catch(IOException e) {
				System.err.println("Got IO exception while writing data" + e);
				System.exit(-1);
			}

		} catch (NumberFormatException e) {
			System.err.println("Unable to parse number: " + e);
			System.exit(-1);
		}
	}

	/**
	 * Generate a line
	 * 
	 * @param size
	 * @param dimension
	 * @param writer 
	 * @throws IOException 
	 */
	protected void generateLine(final int size, final int dimension, final Writer writer)  {
		try {
			final String randomBBox = getRandomBoundingBox(dimension);
			final String randomData = getRandomString(size);
			final String line = String.format("%s %s\n", randomBBox, randomData);
			writer.write(line);
		} catch (IOException e) {
			System.err.println("Got IO exception while writing data" + e);
			System.exit(-1);
		}
	}

	/**
	 * Get a new random bounding box for n dimensions
	 * @param dimension
	 * @return
	 */
	protected String getRandomBoundingBox(final int dimension) {
		final List<Double> bboxData = new ArrayList<>();
		
		for(int i = 0; i < dimension; i++) {
			final double val1 = random.nextDouble() * 100;
			final double val2 = random.nextDouble() * 100;
			
			bboxData.add(Math.min(val1, val2));
			bboxData.add(Math.max(val1, val2));
		}
		
		return bboxData
				.stream()
				.map(d -> Double.toString(d))
				.collect(Collectors.joining(","));
	}

	/**
	 * Get a new random string
	 * 
	 * @param size
	 * @return
	 */
	public String getRandomString(final int size) {
		final StringBuilder sb = new StringBuilder(size);

		for (int i = 0; i < size; i++) {
			sb.append(symbols[random.nextInt(symbols.length)]);
		}

		return sb.toString();
	}

	/**
	 * Check the required args
	 * 
	 * @param requiredArgs
	 */
	protected static void checkRequiredArgs(final List<String> requiredArgs, final CommandLine line) {

		for (final String arg : requiredArgs) {
			if (!line.hasOption(arg)) {
				System.err.println("Option is missing: " + arg);
				printHelpAndExit();
			}
		}
	}

	/**
	 * Main * Main * Main * Main * Main
	 * 
	 * @param args
	 */
	public static void main(final String[] args) {
		try {
			final Options options = buildOptions();
			final CommandLineParser parser = new DefaultParser();
			final CommandLine line = parser.parse(options, args);

			if (line.hasOption(Parameter.HELP)) {
				printHelpAndExit();
			}

			final List<String> requiredArgs = Arrays.asList(Parameter.AMOUNT, 
					Parameter.DIMENSION, Parameter.SIZE, Parameter.OUTPUTFILE);
			
			checkRequiredArgs(requiredArgs, line);

			final SyntheticDataGenerator syntheticDataGenerator = new SyntheticDataGenerator(line);
			syntheticDataGenerator.run();
		} catch (ParseException e) {
			System.err.println("Unable to parse commandline arguments: " + e);
			System.exit(-1);
		}
	}

}
