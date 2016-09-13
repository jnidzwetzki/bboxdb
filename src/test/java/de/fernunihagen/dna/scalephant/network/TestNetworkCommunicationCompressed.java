package de.fernunihagen.dna.scalephant.network;

public class TestNetworkCommunicationCompressed extends TestNetworkCommunication {

	@Override
	protected boolean compressPackages() {
		return true;
	}
	
}
