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
		 * The dimension of the data
		 */
		public final static String DIMENSION = "dimension";

		/**
		 * The number of tuples to produce
		 */
		public final static String LINES = "lines";

		/**
		 * The size per tuple
		 */
		public final static String SIZE = "size";

		/**
		 * The output file
		 */
		public final static String OUTPUTFILE = "outputfile";
		
		/**
		 * The bounding box type
		 */
		public final static String BBOX_TYPE = "bboxtype";
		
		/**
		 * Print help
		 */
		public final static String HELP = "help";
	}
	
	enum BBoxType {
		POINT,
		RANGE;
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
	protected final static Random random = new Random();

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

		// Lines
		final Option lines = Option
				.builder(Parameter.LINES)
				.hasArg()
				.argName("lines")
				.desc("The lines of tuples to produce")
				.build();
		options.addOption(lines);

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
		
		// BBox type
		final Option bboxtype = Option.builder(Parameter.BBOX_TYPE)
				.hasArg()
				.argName("range|point")
				.desc("The type of the bounding box")
				.build();
		options.addOption(bboxtype);
	
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
			final long lines = Long.parseLong(line.getOptionValue(Parameter.LINES));
			final int size = Integer.parseInt(line.getOptionValue(Parameter.SIZE));
			final int dimension = Integer.parseInt(line.getOptionValue(Parameter.DIMENSION));
			final String outputFile = line.getOptionValue(Parameter.OUTPUTFILE);
			final BBoxType bboxType = getBBoxType();
			
			System.out.format("Generating %d lines with %d bytes and %d dimensions\n", lines, size, dimension);

			final File file = new File(outputFile);
			if(file.exists()) {
				System.err.println("File " + outputFile + " already exists, exiting");
				System.exit(-1);
			}
			
			try(final BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
				LongStream.range(0, lines).forEach(l -> generateLine(size, dimension, writer, bboxType));
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
	 * Read the bbox type from args
	 * @return
	 */
	protected BBoxType getBBoxType() {
		final String bboxTypeString = line.getOptionValue(Parameter.BBOX_TYPE);
		if("range".equals(bboxTypeString)) {
			return BBoxType.RANGE;
		} else if("point".equals(bboxTypeString)) {
			return BBoxType.POINT;
		} else {
			System.err.println("Unkown bbox type: " + bboxTypeString);
			System.exit(-1);
		}
		
		// Unreachable code
		return null;
	}
	
	/**
	 * Generate a line
	 * 
	 * @param size
	 * @param dimension
	 * @param writer 
	 * @param bboxType 
	 * @throws IOException 
	 */
	protected void generateLine(final int size, final int dimension, final Writer writer, 
			final BBoxType bboxType)  {
		try {
			final String randomBBox = getRandomBoundingBox(dimension, bboxType);
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
	 * @param bboxType 
	 * @return
	 */
	protected String getRandomBoundingBox(final int dimension, final BBoxType bboxType) {
		final List<Double> bboxData = new ArrayList<>();
		
		for(int i = 0; i < dimension; i++) {
			
			if(bboxType == BBoxType.POINT) {
				final double point = random.nextDouble() * 100;
				
				bboxData.add(point); // Begin
				bboxData.add(point); // End
			} else if(bboxType == BBoxType.RANGE) {
				final double begin = random.nextDouble() * 100;
				final double end = begin + random.nextDouble() * 10;
				
				bboxData.add(begin);
				bboxData.add(end);
			} else {
				throw new IllegalArgumentException("Unknown bbox type: " + bboxType);
			}
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
	public static String getRandomString(final int size) {
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

			final List<String> requiredArgs = Arrays.asList(Parameter.LINES, 
					Parameter.DIMENSION, Parameter.SIZE, Parameter.OUTPUTFILE,
					Parameter.BBOX_TYPE);
			
			checkRequiredArgs(requiredArgs, line);

			final SyntheticDataGenerator syntheticDataGenerator = new SyntheticDataGenerator(line);
			syntheticDataGenerator.run();
		} catch (ParseException e) {
			System.err.println("Unable to parse commandline arguments: " + e);
			System.exit(-1);
		}
	}

}
