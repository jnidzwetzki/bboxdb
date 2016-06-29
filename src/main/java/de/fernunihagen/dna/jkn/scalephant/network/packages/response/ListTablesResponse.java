package de.fernunihagen.dna.jkn.scalephant.network.packages.response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkResponsePackage;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.SSTableName;

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
			
			final ByteBuffer bb = ByteBuffer.allocate(2);
			bb.order(NetworkConst.NETWORK_BYTEORDER);
			bb.putShort((short) bodyBytes.length);
			
			// Write body length
			final int bodyLength = bb.capacity() + bodyBytes.length;
			final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(8);
			bodyLengthBuffer.order(NetworkConst.NETWORK_BYTEORDER);
			bodyLengthBuffer.putLong(bodyLength);
			bos.write(bodyLengthBuffer.array());

			// Write body
			bos.write(bb.array());
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
		
		for(final SSTableName table : tables) {
			final byte[] tableBytes = table.getFullnameBytes();
			bodyStream.write(tableBytes, 0, tableBytes.length);
			bodyStream.write('\0');
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
		final List<SSTableName> tables = new ArrayList<SSTableName>();

		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(encodedPackage, NetworkConst.RESPONSE_LIST_TABLES);

		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
		// Read the body length
		encodedPackage.getShort();
		
		StringBuilder sb = new StringBuilder();
		while(encodedPackage.remaining() != 0) {
			final byte currentByte = encodedPackage.get();
			
			// Got terminal, tablename is complete
			if(currentByte == '\0') {
				tables.add(new SSTableName(sb.toString()));
				sb = new StringBuilder();
			} else {
				sb.append((char) currentByte);
			}
		}
		
		if(sb.length() != 0) {
			logger.warn("Body read complete, but buffer not empty. Is the last table terminated by \\0?" + sb.toString());
		}
		
		return new ListTablesResponse(requestId, tables);
	}
}
