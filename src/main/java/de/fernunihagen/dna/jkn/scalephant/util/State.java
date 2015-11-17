package de.fernunihagen.dna.jkn.scalephant.util;

public class State {

	protected volatile boolean ready;

	public State() {
		ready = false;
	}
	
	public State(boolean ready) {
		this.ready = ready;
	}
	
	public boolean isReady() {
		return ready;
	}

	public void setReady(boolean ready) {
		this.ready = ready;
	}

	@Override
	public String toString() {
		return "Ready [ready=" + ready + "]";
	}
	
}
