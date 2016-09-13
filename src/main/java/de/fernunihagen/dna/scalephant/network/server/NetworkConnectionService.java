package de.fernunihagen.dna.scalephant.network.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.scalephant.ScalephantService;
import de.fernunihagen.dna.scalephant.util.State;

public class NetworkConnectionService implements ScalephantService {
	
	/**
	 * The configuration
	 */
	protected final ScalephantConfiguration configuration = ScalephantConfigurationManager.getConfiguration();

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
	 * The state of the server (readonly or readwrite)
	 */
	protected final NetworkConnectionServiceState networkConnectionServiceState = new NetworkConnectionServiceState();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(NetworkConnectionService.class);
	
	/**
	 * Start the network connection handler
	 */
	public void init() {
		
		if(state.isReady()) {
			logger.info("init() called on ready instance, ignoring");
			return;
		}
		
		logger.info("Start the network connection handler on port: " + configuration.getNetworkListenPort());
		
		if(threadPool == null) {
			threadPool = Executors.newFixedThreadPool(configuration.getNetworkConnectionThreads());
		}
		
		networkConnectionServiceState.setReadonly(true);
		
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
		
		/**
		 * The server socket
		 */
		private ServerSocket serverSocket;

		@Override
		public void run() {
			
			logger.info("Starting new connection dispatcher");
			
			try {
				serverSocket = new ServerSocket(configuration.getNetworkListenPort());
				serverSocket.setReuseAddress(true);
				
				while(! shutdown) {
					final Socket clientSocket = serverSocket.accept();
					handleConnection(clientSocket);
				}
				
			} catch(IOException e) {
				logger.error("Got an IO exception while reading from server socket ", e);
				shutdown = true;
			} finally {
				closeSocketNE();
			}
			
			logger.info("Shutting down the connection dispatcher");
		}

		/**
		 * Close socket without an exception
		 */
		protected void closeSocketNE() {
			if(serverSocket != null) {
				try {
					logger.info("Close server socket on port: " + serverSocket.getLocalPort());
					serverSocket.close();
					serverSocket = null;
				} catch (IOException e) {
					// Ignore close exception
				}
			}
		}
		
		/**
		 * Set the shutdown flag
		 * @param shutdown
		 */
		public void setShutdown(final boolean shutdown) {
			this.shutdown = shutdown;
			
			if(shutdown == true) {
				closeSocketNE();
			}
		}
		
		/**
		 * Dispatch the connection to the thread pool
		 * @param clientSocket
		 */
		protected void handleConnection(final Socket clientSocket) {
			logger.debug("Got new connection from: " + clientSocket.getInetAddress());
			threadPool.submit(new ClientConnectionHandler(clientSocket, networkConnectionServiceState));
		}
	}


	@Override
	public String getServicename() {
		return "Network connection handler";
	}
	
	/**
	 * Set the readonly mode
	 * @param readonly
	 */
	public void setReadonly(final boolean readonly) {
		networkConnectionServiceState.setReadonly(readonly);
	}

	/**
	 * Get the readonly mode
	 * @return
	 */
	public boolean isReadonly() {
		return networkConnectionServiceState.isReadonly();
	}
}