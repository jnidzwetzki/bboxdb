package org.bboxdb.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.bboxdb.commons.MathUtil;

public class SplitFile implements Runnable {

	/**
	 * The file to process
	 */
	private final String filename;
	
	/**
	 * The output dir
	 */
	private final String outputDir;
	
	/**
	 * The lines per batch
	 */
	private final int linesPerBatch;
	
	/**
	 * The number of files
	 */
	private final int numberOfFiles;
	
	/**
	 * The current lines in the batch
	 */
	private final AtomicLong linesInBatch = new AtomicLong(0);
	
	/**
	 * The current batch
	 */
	private final AtomicInteger currentBatch = new AtomicInteger(0);
	
	/**
	 * The output file writer
	 */
	private final List<BufferedWriter> writers = new ArrayList<>();

	public SplitFile(final String filename, final String outputDir, final int linesPerBatch, final int numberOfFiles) {
		this.filename = filename;
		this.outputDir = outputDir;
		this.linesPerBatch = linesPerBatch;
		this.numberOfFiles = numberOfFiles;
	}
	
	@Override
	public void run() {
	
		openFiles();
		
        try (final Stream<String> stream = Files.lines(Paths.get(filename))) {
            stream.forEach(l -> handleLine(l));
        } catch (IOException e) {
            e.printStackTrace();
        }
		
        closeFiles();
        
	}
	
	/**
	 * Open all fiel writer
	 */
	private void openFiles() {
		for(int fileNo = 0; fileNo < numberOfFiles; fileNo++) {
			
			final File file = new File(outputDir + File.pathSeparator + Integer.toString(fileNo));
			
			try {
				final BufferedWriter writer = new BufferedWriter(new FileWriter(file));
				writers.add(writer);
			} catch (IOException e) {
				System.err.println("Unable to open file for writing: " + file);
				e.printStackTrace();
				System.exit(-3);
			}
		}
	}

	/**
	 * Close all file writer
	 */
	private void closeFiles() {
		for(final BufferedWriter writer : writers) {
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		writers.clear();
	}

	/**
	 * Process the lines of the file
	 * @param line
	 */
	private void handleLine(final String line) {
		
		try {
			linesInBatch.incrementAndGet();
			
			final BufferedWriter writer = writers.get(currentBatch.get());
			writer.write(line);
			writer.write(System.lineSeparator());
		} catch (IOException e) {
			System.err.println("Unable to write line to file");
			e.printStackTrace();
			System.exit(-4);
		}
		
		if(linesInBatch.get() >= linesPerBatch) {
			final int curBatchValue = currentBatch.incrementAndGet();
			currentBatch.set(curBatchValue % numberOfFiles);
		}
	}

	/**
	 * Main Main Main Main
	 * @param args
	 */
	public static void main(final String[] args) {
		
		if(args.length != 4) {
			System.err.println("Usage: <Program> <Filename> <OutputDir> <Lines per Batch> <Files>");
			System.exit(-1);
		}
		
		final String filename = args[0];
		final String outputDir = args[1];
		final String linesPerBatchString = args[2];
		final String numberOfFilesString = args[3];
		
		final File file = new File(filename);
		
		if(! file.canRead()) {
			System.err.println("Unable to read file: " + file);
			System.exit(-1);
		}
		
		final File outputdirFile = new File(outputDir);
		if(outputdirFile.exists()) {
			System.err.println("Output dir already exists, please remove: " + outputdirFile);
			System.exit(-2);
		}
		
		final int linesPerBatch = MathUtil.tryParseIntOrExit(linesPerBatchString, () -> "Unable to parse as int: " + linesPerBatchString);
		final int numberOfFiles = MathUtil.tryParseIntOrExit(numberOfFilesString, () -> "Unable to parse as int: " + numberOfFilesString);
		
		final SplitFile splitFile = new SplitFile(filename, outputDir, linesPerBatch, numberOfFiles);
		splitFile.run();
	}

}
