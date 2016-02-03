package de.fernunihagen.dna.jkn.scalephant.network;

import java.util.concurrent.atomic.AtomicInteger;

public class SequenceNumberGenerator {

	/**
	 * The next unused free request id
	 */
	protected final AtomicInteger sequenceNumber = new AtomicInteger();
	
	public SequenceNumberGenerator() {
		sequenceNumber.set(0);
	}
	
	/**
	 * Get the next free sequence number
	 * @return
	 */
	public short getNextSequenceNummber() {
		return (short) sequenceNumber.getAndIncrement();
	}
}
