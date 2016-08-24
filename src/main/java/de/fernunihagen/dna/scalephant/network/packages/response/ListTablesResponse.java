package de.fernunihagen.dna.scalephant.network.packages.response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.network.NetworkConst;
import de.fernunihagen.dna.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.scalephant.network.packages.NetworkResponsePackage;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.tools.DataEncoderHelper;

public class ListTablesResponse extends NetworkResponsePackage {
	
	/**
	 * The tables
	 */
	protected final List<SSTableName> tables;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ListTablesResponse.class);

	public ListTablesResponse(final short sequenceNumber, final List<SSTableName> allTables) {
		super(sequenceNumber);
		this.tables = allTables;
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.RESPONSE_LIST_TABLES;
	}

	@Override
	public byte[] getByteArray() {
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder();
	
		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForResponsePackage(sequenceNumber, getPackageType());
		
		try {
			final byte[] bodyBytes = createBody();
			
			// Calculate and write body length
			final int bodyLength = bodyBytes.length;			
			final ByteBuffer bodyLengthBuffer = DataEncoderHelper.longToByteBuffer(bodyLength);			
			bos.write(bodyLengthBuffer.array());

			// Write body
			bos.write(bodyBytes);
			bos.close();
		} catch (IOException e) {
			logger.error("Got exception while converting package into bytes", e);
			return null;
		}
	
		return bos.toByteArray();
	}

	/**
	 * Create the body as byte array
	 * @return
	 * @throws IOException
	 */
	protected byte[] createBody() throws IOException {
		final ByteArrayOutputStream bodyStream = new ByteArrayOutputStream();
		
		// Write total amount of tables
		final ByteBuffer totalTables = DataEncoderHelper.intToByteBuffer(tables.size());			
		bodyStream.write(totalTables.array(), 0, totalTables.array().length);
		
		for(final SSTableName table : tables) {
			final byte[] tableBytes = table.getFullnameBytes();
			
			// Write table length
			final ByteBuffer tableLength = DataEncoderHelper.shortToByteBuffer((short) tableBytes.length);			
			bodyStream.write(tableLength.array(), 0, tableLength.array().length);
			
			// Write table name
			bodyStream.write(tableBytes, 0, tableBytes.length);
		}
		bodyStream.close();
		
		return bodyStream.toByteArray();
	}

	/**
	 * Returns the relations of this request
	 * @return
	 */
	public List<SSTableName> getTables() {
		return Collections.unmodifiableList(tables);
	}

	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static ListTablesResponse decodePackage(final ByteBuffer encodedPackage) {		
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);

		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(encodedPackage, NetworkConst.RESPONSE_LIST_TABLES);

		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
		// Read the total amount of tables
		final int totalTables = encodedPackage.getInt();
		final List<SSTableName> tables = new ArrayList<SSTableName>(totalTables);

		// Read and decode tables
		for(short readTables = 0; readTables < totalTables; readTables++) {
			// Read table name length
			final short tableNameLength = encodedPackage.getShort();
			final byte[] tablenameBytes = new byte[tableNameLength];
			
			// Read table name and decode
			encodedPackage.get(tablenameBytes, 0, tablenameBytes.length);
			final String tablename = new String(tablenameBytes);
			final SSTableName sstableName = new SSTableName(tablename);
			tables.add(sstableName);
		}
		
		if(encodedPackage.remaining() != 0) {
			logger.error("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new ListTablesResponse(requestId, tables);
	}
}
