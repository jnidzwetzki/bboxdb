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

public class ListTablesResponse extends NetworkResponsePackage {
	
	/**
	 * The tables
	 */
	protected final List<String> tables;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ListTablesResponse.class);

	public ListTablesResponse(final short sequenceNumber, final List<String> tables) {
		super(sequenceNumber);
		this.tables = tables;
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
			final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(4);
			bodyLengthBuffer.order(NetworkConst.NETWORK_BYTEORDER);
			bodyLengthBuffer.putInt(bodyLength);
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
		
		for(final String table : tables) {
			final byte[] tableBytes = table.getBytes();
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
	public List<String> getTables() {
		return Collections.unmodifiableList(tables);
	}

	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static ListTablesResponse decodeTuple(final byte encodedPackage[]) {
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(bb);
		final List<String> tables = new ArrayList<String>();

		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(bb, NetworkConst.RESPONSE_LIST_TABLES);

		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
		// Read the body length
		bb.getShort();
		
		StringBuilder sb = new StringBuilder();
		while(bb.remaining() != 0) {
			final byte currentByte = bb.get();
			
			// Got terminal, tablename is complete
			if(currentByte == '\0') {
				tables.add(sb.toString());
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
