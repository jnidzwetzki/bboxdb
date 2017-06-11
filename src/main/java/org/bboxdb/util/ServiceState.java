/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *    
 *******************************************************************************/
package org.bboxdb.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.google.common.base.Throwables;

public class ServiceState {
	
	public enum State {		
		NEW(1), 
		STARTING(2), // Transition state
		RUNNING(3),
		STOPPING(4), // Transition state
		TERMINATED(5),
		FAILED(6);
		
		protected final int id;

		private State(final int id) {
			this.id = id;
		}
		
		public int getId() {
			return id;
		}
	}
	
	/**
	 * The current state
	 */
	protected State state;

	/**
	 * The reason for the failed state
	 */
	protected Throwable throwable;
	
	/**
	 * Callback handler
	 */
	protected final List<Consumer<? super State>> callbacks;
	
	public ServiceState() {
		callbacks = new ArrayList<>();
		reset();
	}
	
	/**
	 * Set a new state
	 * @param state
	 */
	protected void setNewState(final State state) {
		this.state = state;
		
		callbacks.forEach(c -> c.accept(state));
		
		synchronized (this) {
			this.notifyAll();
		}
	}
	
	/**
	 * Reset the state
	 */
	public void reset() {
		setNewState(State.NEW);
	}

	/**
	 * Dispatch to the running state
	 */
	public void dipatchToStarting() {
		
		if(state == State.FAILED) {
			return;
		}
		
		if(state != State.NEW) {
			throw new IllegalStateException("State is not new: " + state);
		}
		
		setNewState(State.STARTING);
	}
	
	/**
	 * Dispatch to the starting state
	 */
	public void dispatchToRunning() {
		
		if(state == State.FAILED) {
			return;
		}
		
		if(state != State.STARTING) {
			throw new IllegalStateException("State is not starting: " + state);
		}
		
		setNewState(State.RUNNING);
	}
	
	/**
	 * Dispatch to the stopping state
	 */
	public void dispatchToStopping() {
		
		if(state == State.FAILED) {
			return;
		}
		
		if(state != State.RUNNING) {
			throw new IllegalStateException("State is not stopping: " + state);
		}
		
		setNewState(State.STOPPING);
	}
	
	/**
	 * Dispatch to the terminated state
	 */
	public void dispatchToTerminated() {
		
		if(state == State.FAILED) {
			return;
		}
		
		if(state != State.STOPPING) {
			throw new IllegalStateException("State is not terminated: " + state);
		}
		
		setNewState(State.TERMINATED);
	}
	
	/**
	 * Dispatch to the failed state
	 */
	public void dispatchToFailed(final Throwable throwable) {		
		this.throwable = throwable;
		
		setNewState(State.FAILED);
	}
	
	/**
	 * Await at leat the starting state
	 * @throws InterruptedException
	 */
	public void awaitStarting() throws InterruptedException {
		synchronized (this) {
			while(state.getId() < State.STARTING.getId()) {
				this.wait();
			}
		}
	}
	
	/**
	 * Await at leat the running state
	 * @throws InterruptedException
	 */
	public void awaitRunning() throws InterruptedException {
		synchronized (this) {
			while(state.getId() < State.RUNNING.getId()) {
				this.wait();
			}
		}
	}
	
	/**
	 * Await at leat the stopping state
	 * @throws InterruptedException
	 */
	public void awaitStopping() throws InterruptedException {
		synchronized (this) {
			while(state.getId() < State.STOPPING.getId()) {
				this.wait();
			}
		}
	}
	
	/**
	 * Await at leat the terminated state
	 * @throws InterruptedException
	 */
	public void awaitTerminatedOrFailed() throws InterruptedException {
		synchronized (this) {
			while(state != State.TERMINATED && state != State.FAILED) {
				this.wait();
			}
		}
	}
	
	/**
	 * Is the service is in a finished state
	 */
	public boolean isInFinishedState() {
		return state == State.TERMINATED || state == State.FAILED;
	}
	
	/**
	 * Is this in running state
	 * @return
	 */
	public boolean isInRunningState() {
		return state == State.RUNNING;
	}
	
	/**
	 * Is this in shutdown state
	 * @return 
	 */
	public boolean isInShutdownState() {
		return state.getId() > State.RUNNING.getId();
	}
	
	/**
	 * Is this in new state
	 * @return
	 */
	public boolean isInNewState() {
		return state == State.NEW;
	}

	@Override
	public String toString() {
		return "ServiceState [state=" + state + ", throwable=" 
				+ getThrowableAsString() + "]";
	}

	/**
	 * Get the state
	 * @return
	 */
	public State getState() {
		return state;
	}
	
	/**
	 * Get the throwable
	 * @return
	 */
	public Throwable getThrowable() {
		return throwable;
	}
	
	/**
	 * Get the throwable as string
	 * @return
	 */
	public String getThrowableAsString() {
		if(throwable == null) {
			return "";
		}
		
		return Throwables.getStackTraceAsString(throwable);
	}
	
	/**
	 * Register a callback listener
	 * @param c
	 */
	public void registerCallback(final Consumer<? super State> consumer) {
		callbacks.add(consumer);
	}
	
	/**
	 * Remove a callback listener
	 * @param consumer
	 * @return
	 */
	public boolean removeCallback(final Consumer<? super State> consumer) {
		return callbacks.remove(consumer);
	}
}
