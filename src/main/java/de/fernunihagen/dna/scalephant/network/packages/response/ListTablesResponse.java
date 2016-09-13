package de.fernunihagen.dna.scalephant.network.packages.response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import de.fernunihagen.dna.scalephant.network.NetworkConst;
import de.fernunihagen.dna.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.scalephant.network.packages.NetworkResponsePackage;
import de.fernunihagen.dna.scalephant.network.packages.PackageEncodeError;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.tools.DataEncoderHelper;

public class ListTablesResponse extends NetworkResponsePackage {
	
	/**
	 * The tables
	 */
	protected final List<SSTableName> tables;

	public ListTablesResponse(final short sequenceNumber, final List<SSTableName> allTables) {
		super(sequenceNumber);
		this.tables = allTables;
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.RESPONSE_TYPE_LIST_TABLES;
	}

	@Override
	public byte[] getByteArray() throws PackageEncodeError {
		final NetworkPackageEncoder networkPackageEncoder = new NetworkPackageEncoder();
	
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
			throw new PackageEncodeError("Got exception while converting package into bytes", e);
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
	 * @throws PackageEncodeError 
	 */
	public static ListTablesResponse decodePackage(final ByteBuffer encodedPackage) throws PackageEncodeError {		
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);

		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(encodedPackage, NetworkConst.RESPONSE_TYPE_LIST_TABLES);

		if(decodeResult == false) {
			throw new PackageEncodeError("Unable to decode package");
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
			throw new PackageEncodeError("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new ListTablesResponse(requestId, tables);
	}
}
