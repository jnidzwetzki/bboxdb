package de.fernunihagen.dna.scalephant.network.packages.response;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.Const;
import de.fernunihagen.dna.scalephant.network.NetworkConst;
import de.fernunihagen.dna.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.scalephant.network.packages.NetworkResponsePackage;

public class CompressionEnvelopeResponse extends NetworkResponsePackage {

	/**
	 * The package to encode
	 */
	protected NetworkResponsePackage networkResponsePackage;
	
	/**
	 * The compression type
	 */
	protected byte compressionType;

	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(CompressionEnvelopeResponse.class);

	public CompressionEnvelopeResponse(final NetworkResponsePackage networkResponsePackage, final byte compressionType) {
		super(networkResponsePackage.getSequenceNumber());

		this.networkResponsePackage = networkResponsePackage;
		this.compressionType = compressionType;
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.RESPONSE_TYPE_COMPRESSION;
	}

	@Override
	public byte[] getByteArray() {
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder();
	
		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForResponsePackage(sequenceNumber, getPackageType());
		
		try {
			if(compressionType != NetworkConst.COMPRESSION_TYPE_GZIP) {
				logger.error("Unknown compression method: " + compressionType);
				return null;
			}
			
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final OutputStream os = new GZIPOutputStream(baos);
			final byte[] uncompressedBytes = networkResponsePackage.getByteArray();
			os.write(uncompressedBytes);
			os.close();
			final byte[] compressedBytes = baos.toByteArray();
			
			final ByteBuffer bb = ByteBuffer.allocate(5);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			bb.putInt(compressedBytes.length);
			bb.put(compressionType);
			
			// Body length
			final long bodyLength = bb.capacity() + compressedBytes.length;

			// Write body length
			final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(8);
			bodyLengthBuffer.order(Const.APPLICATION_BYTE_ORDER);
			bodyLengthBuffer.putLong(bodyLength);
			bos.write(bodyLengthBuffer.array());
			
			// Write body
			bos.write(bb.array());
			bos.write(compressedBytes);
			
			bos.close();
		} catch (IOException e) {
			logger.error("Got exception while converting package into bytes", e);
			return null;
		}
	
		return bos.toByteArray();
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws IOException 
	 */
	public static byte[] decodePackage(final ByteBuffer encodedPackage) throws IOException {
		
		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(encodedPackage, NetworkConst.RESPONSE_TYPE_COMPRESSION);
		
		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
		final int compressedDataLength = encodedPackage.getInt();
		final byte compressionType = encodedPackage.get();
		
		if(compressionType != NetworkConst.COMPRESSION_TYPE_GZIP) {
			logger.error("Unknown compression type: " + compressionType);
			return null;
		}
		
		if(compressedDataLength != encodedPackage.remaining()) {
			logger.error("Remaning : " + encodedPackage.remaining() + " bytes. But compressed data should have: " + compressedDataLength + " bytes");
			return null;
		}
		
		final byte[] compressedBytes = new byte[compressedDataLength];
		encodedPackage.get(compressedBytes, 0, compressedDataLength);
		
		final ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);
		final GZIPInputStream inputStream = new GZIPInputStream(bais);
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();

		final byte[] buffer = new byte[10240];
		for (int length = 0; (length = inputStream.read(buffer)) > 0; ) {
			baos.write(buffer, 0, length);
		}

		inputStream.close();
		baos.close();
		
		return baos.toByteArray();
	}

}
