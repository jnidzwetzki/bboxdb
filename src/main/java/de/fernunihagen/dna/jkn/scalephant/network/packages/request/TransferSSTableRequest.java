package de.fernunihagen.dna.jkn.scalephant.network.packages.request;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkRequestPackage;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableHelper;


public class TransferSSTableRequest implements NetworkRequestPackage {

	/**
	 * The name of the table
	 */
	protected final SSTableName table;
	
	/**
	 * The meta data
	 */
	protected final File metadata;
	
	/**
	 * The SStable
	 */
	protected final File sstable;
	
	/**
	 * The key index
	 */
	protected final File keyIndex;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TransferSSTableRequest.class);
	
	public TransferSSTableRequest(final String table, final File metadata, final File sstable, final File keyIndex) {
		super();
		this.table = new SSTableName(table);
		this.metadata = metadata;
		this.sstable = sstable;
		this.keyIndex = keyIndex;
	}

	/**
	 * Get the a encoded version of this class
	 */
	@Override
	public void writeToOutputStream(final short sequenceNumber, final OutputStream outputStream) {
		
		NetworkPackageEncoder.appendRequestPackageHeader(sequenceNumber, getPackageType(), outputStream);

		try {
			final byte[] tableBytes = table.getFullnameBytes();
			
			final ByteBuffer bb = ByteBuffer.allocate(28);
			bb.order(NetworkConst.NETWORK_BYTEORDER);
			bb.putShort((short) tableBytes.length);
			bb.put(NetworkConst.UNUSED_BYTE);
			bb.put(NetworkConst.UNUSED_BYTE);
		
			bb.putLong(metadata.length());
			bb.putLong(sstable.length());
			bb.putLong(keyIndex.length());
			
			// Write body length
			final long bodyLength = bb.capacity() + tableBytes.length 
					+ metadata.length() + sstable.length() + keyIndex.length();
			
			final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(8);
			bodyLengthBuffer.order(NetworkConst.NETWORK_BYTEORDER);
			bodyLengthBuffer.putLong(bodyLength);
			outputStream.write(bodyLengthBuffer.array());
			
			// Write body
			outputStream.write(bb.array());
			outputStream.write(tableBytes);
			
			// Write the data onto output stream
			writeFileToOutput(outputStream, metadata);
			writeFileToOutput(outputStream, sstable);
			writeFileToOutput(outputStream, keyIndex);
		} catch (IOException e) {
			logger.error("Got exception while converting package into bytes", e);
		}		
	}

	/**
	 * Write the given file handle onto the output stream
	 * @param outputStream
	 * @param filename
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected void writeFileToOutput(final OutputStream outputStream, final File filename)
			throws FileNotFoundException, IOException {
		
		final BufferedInputStream bis = new BufferedInputStream(new FileInputStream(filename));
		final long filesize = metadata.length();
		final byte[] byteBuffer = new byte[2048];
		
		long totalReadBytes = 0;
		
		while (totalReadBytes == filesize) {
			final int readBytes = bis.read(byteBuffer);
			totalReadBytes = totalReadBytes + readBytes;
			outputStream.write(byteBuffer, 0, readBytes);
		}
		
		bis.close();
	}
	
	/**
	 * Decode the encoded package into an object
	 * 
	 * @param encodedPackage
	 * @param bodyLength 
	 * @param inputStream 
	 * @return
	 * @throws IOException 
	 */
	public static boolean decodeTuple(final ByteBuffer packageHeader, final long bodyLength, 
			final ScalephantConfiguration configuration, final InputStream inputStream) throws IOException {
		
		final String dataDirectory = configuration.getDataDirectory();
		
		// Read the package until the data streams (4 Byte Tablename, 3x8 Byte Datalength = 28 Byte)
		final int partialBodyLength = packageHeader.limit() + 28;
		final ByteBuffer partialPackage = ByteBuffer.allocate(partialBodyLength);
		partialPackage.put(packageHeader.array());
		inputStream.read(partialPackage.array(), partialPackage.position(), partialBodyLength);

		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(partialPackage, NetworkConst.REQUEST_TYPE_TRANSFER);
		
		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return false;
		}
		
		final short tableLength = partialPackage.getShort();

		// 2 unused bytes
		partialPackage.get();
		partialPackage.get();
		
		// Read the file length
		final long metadataLength = partialPackage.getLong();
		final long sstableLength = partialPackage.getLong();
		final long indexLength = partialPackage.getLong();
				
		if(partialPackage.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + partialPackage.remaining());
			return false;
		}
		
		final byte[] tableBytes = new byte[tableLength];
		inputStream.read(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		final File directoryHandle = new File(SSTableHelper.getSSTableDir(dataDirectory, table));
		
		if( ! checkDirectoryAndCreate(directoryHandle) ) {
			return false;
		}
		
		// Read the metadata
		final File metadataFile = new File(SSTableHelper.getSSTableMetadataFilename(dataDirectory, table, 1));
		readStreamIntoFile(inputStream, metadataLength, metadataFile);
		
		// Read the sstable
		final File sstableFile = new File(SSTableHelper.getSSTableFilename(dataDirectory, table, 1));
		readStreamIntoFile(inputStream, sstableLength, sstableFile);
		
		// Key index
		final File sstableIndex = new File(SSTableHelper.getSSTableIndexFilename(dataDirectory, table, 1));
		readStreamIntoFile(inputStream, indexLength, sstableIndex);
		
		return true;
	}

	/**
	 * Verify that the output directory does not exist and create the directory.
	 * 
	 * @param directoryHandle
	 * @return
	 */
	protected static boolean checkDirectoryAndCreate(final File directoryHandle) {
		if(directoryHandle.exists()) {
			logger.error("Directory " + directoryHandle + " already exists");
			return false;
		}
		
		logger.info("Create new directory: " + directoryHandle);
		directoryHandle.mkdir();
		
		return true;
	}

	/**
	 * Read the the amount of dataLength bytes from the network
	 * stream and write it into the output file
	 * 
	 * @param inputStream
	 * @param dataLength
	 * @param outputFile
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected static void readStreamIntoFile(final InputStream inputStream,
			final long dataLength, final File outputFile)
			throws FileNotFoundException, IOException {
		final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile));
		
		int totalReadBytes = 0;
		final byte[] byteBuffer = new byte[2048];
		
		while(totalReadBytes < dataLength) {
			final int readBytes = inputStream.read(byteBuffer, 0, byteBuffer.length);
			bos.write(byteBuffer, 0, readBytes);
			totalReadBytes = totalReadBytes + readBytes;
		}
		
		bos.close();
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_TRANSFER;
	}

	public SSTableName getTable() {
		return table;
	}

}
