package de.fernunihagen.dna.jkn.scalephant.network.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Lifecycle;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.util.State;

public class ConnectionHandler implements Lifecycle {
	
	/**
	 * Our thread pool to handle connections
	 */
	protected ExecutorService threadPool;
	
	/**
	 * The connection handler state
	 */
	protected final State state = new State(false);
	
	/**
	 * The connection dispatcher runnable
	 */
	protected ConnectionDispatcher serverSocketDispatcher = null;
	
	/**
	 * The thread that listens on the server socket and dispatches
	 * incoming requests to the thread pool
	 */
	protected Thread serverSocketDispatchThread = null;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ConnectionHandler.class);
	
	/**
	 * Start the network connection handler
	 */
	public void init() {
		
		if(state.isReady()) {
			logger.info("init() called on ready instance, ignoring");
			return;
		}
		
		logger.info("Start the network connection handler on port: " + NetworkConst.NETWORK_PORT);
		
		if(threadPool == null) {
			threadPool = Executors.newFixedThreadPool(NetworkConst.SERVER_CONNECTION_THREADS);
		}
		
		serverSocketDispatcher = new ConnectionDispatcher();
		serverSocketDispatchThread = new Thread(serverSocketDispatcher);
		serverSocketDispatchThread.start();
		serverSocketDispatchThread.setName("Connection dispatcher thread");
		
		state.setReady(true);
	}
	
	/**
	 * Shutdown our thread pool
	 */
	public void shutdown() {
		
		logger.info("Shutdown the network connection handler");
		
		if(serverSocketDispatchThread != null) {
			serverSocketDispatcher.setShutdown(true);
			serverSocketDispatchThread.interrupt();
			
			serverSocketDispatchThread = null;
			serverSocketDispatcher = null;
		}
		
		if(threadPool != null) {
			threadPool.shutdown();
		}
		
		threadPool = null;
		state.setReady(false);
	}
	
	@Override
	protected void finalize() throws Throwable {
		shutdown();
		super.finalize();
	}
	
	/**
	 * Is the connection handler ready?
	 * @return
	 */
	public boolean isReady() {
		return state.isReady();
	}
	
	
	/**
	 * The connection dispatcher
	 *
	 */
	class ConnectionDispatcher implements Runnable {

		/**
		 * The shutdown signal
		 */
		protected volatile boolean shutdown = false;

		@Override
		public void run() {
			
			logger.info("Starting new connection dispatcher");
			
			try {
				final ServerSocket serverSocket = new ServerSocket(NetworkConst.NETWORK_PORT);
				
				while(! shutdown) {
					final Socket clientSocket = serverSocket.accept();
					handleConnection(clientSocket);
				}
				
				serverSocket.close();
			} catch(IOException e) {
				logger.error("Unable to start server socket ", e);
				shutdown = true;
			}
			
			logger.info("Shutting down the connection dispatcher");
		}
		
		/**
		 * Set the shutdown flag
		 * @param shutdown
		 */
		public void setShutdown(boolean shutdown) {
			this.shutdown = shutdown;
		}
		
		/**
		 * Dispatch the connection to the thread pool
		 * @param clientSocket
		 */
		protected void handleConnection(final Socket clientSocket) {
			logger.debug("Got new connection from: " + clientSocket.getInetAddress());
			threadPool.submit(new ClientConnectionHandler(clientSocket));
		}
	}
}