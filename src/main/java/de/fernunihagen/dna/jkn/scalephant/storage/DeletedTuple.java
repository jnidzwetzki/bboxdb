package de.fernunihagen.dna.jkn.scalephant.storage;

public class DeletedTuple extends Tuple {

	public DeletedTuple(String key) {
		super(key, null, null);
	}

	@Override
	public String toString() {
		return "DeletedTuple [key=" + key + ", timestamp=" + timestamp + "]";
	}
	
}
