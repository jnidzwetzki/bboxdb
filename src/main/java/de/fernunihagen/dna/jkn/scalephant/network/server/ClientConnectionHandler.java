package de.fernunihagen.dna.jkn.scalephant.network.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Const;
import de.fernunihagen.dna.jkn.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.jkn.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperClient;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperClientFactory;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkConnectionState;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkResponsePackage;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.CreateDistributionGroupRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.DeleteDistributionGroupRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.DeleteTableRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.DeleteTupleRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.InsertTupleRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.QueryBoundingBoxRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.QueryKeyRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.QueryTimeRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.TransferSSTableRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.ErrorResponse;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.ListTablesResponse;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.MultipleTupleEndResponse;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.MultipleTupleStartResponse;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.SuccessResponse;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.TupleResponse;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageInterface;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManager;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;

public class ClientConnectionHandler implements Runnable {
	
	/**
	 * The client socket
	 */
	protected final Socket clientSocket;
	
	/**
	 * The output stream of the socket
	 */
	protected BufferedOutputStream outputStream;
	
	/**
	 * The input stream of the socket
	 */
	protected InputStream inputStream;
	
	/**
	 * The connection state
	 */
	protected volatile NetworkConnectionState connectionState;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ClientConnectionHandler.class);
	

	public ClientConnectionHandler(final Socket clientSocket) {
		this.clientSocket = clientSocket;
		connectionState = NetworkConnectionState.NETWORK_CONNECTION_OPEN;
		
		try {
			outputStream = new BufferedOutputStream(clientSocket.getOutputStream());
			inputStream = new BufferedInputStream(clientSocket.getInputStream());
		} catch (IOException e) {
			inputStream = null;
			outputStream = null;
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
			logger.error("Exception while creating IO stream", e);
		}
	}

	/**
	 * Read the next pakage header from the socket
	 * @return The package header, wrapped in a ByteBuffer
	 * @throws IOException
	 */
	protected ByteBuffer readNextPackageHeader() throws IOException {
		final ByteBuffer bb = ByteBuffer.allocate(12);
		inputStream.read(bb.array(), 0, bb.limit());
		return bb;
	}

	/**
	 * Write a response package to the client
	 * @param responsePackage
	 */
	protected boolean writeResultPackage(final NetworkResponsePackage responsePackage) {
		
		final byte[] outputData = responsePackage.getByteArray();
		
		try {
			outputStream.write(outputData, 0, outputData.length);
			outputStream.flush();
			return true;
		} catch (IOException e) {
			logger.warn("Unable to write result package", e);
		}

		return false;
	}
	
	@Override
	public void run() {
		try {
			logger.debug("Handling new connection from: " + clientSocket.getInetAddress());

			while(connectionState == NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
				handleNextPackage();
			}
			
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
			logger.info("Closing connection to: " + clientSocket.getInetAddress());
		} catch (IOException e) {
			// Ignore exception on closing sockets
			if(connectionState == NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
				logger.error("Unable to read data from socket (state: " + connectionState + ")", e);
			}
		}
		
		try {
			clientSocket.close();
		} catch (IOException e) {
			// Ignore close exception
		}
	}
	
	/**
	 * Handle the delete table call
	 * @param packageSequence 
	 * @return
	 */
	protected boolean handleDeleteTable(final ByteBuffer packageHeader, final short packageSequence) {
		
		final ByteBuffer encodedPackage = readFullPackage(packageHeader);
		
		final DeleteTableRequest deletePackage = DeleteTableRequest.decodeTuple(encodedPackage);
		logger.info("Got delete call for table: " + deletePackage.getTable());
		
		try {
			// Propergate the call to the storage manager
			StorageInterface.deleteTable(deletePackage.getTable().getFullname());
			writeResultPackage(new SuccessResponse(packageSequence));
		} catch (StorageManagerException e) {
			logger.warn("Error while delete tuple", e);
			writeResultPackage(new ErrorResponse(packageSequence));	
		}
		
		return true;
	}
	
	/**
	 * Handle query package
	 * @param bb
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleQuery(final ByteBuffer packageHeader, final short packageSequence) {
		
		final ByteBuffer encodedPackage = readFullPackage(packageHeader);
		final byte queryType = NetworkPackageDecoder.getQueryTypeFromRequest(encodedPackage);

		switch (queryType) {
			case NetworkConst.REQUEST_QUERY_KEY:
				handleKeyQuery(encodedPackage, packageSequence);
				break;
				
			case NetworkConst.REQUEST_QUERY_BBOX:
				handleBoundingBoxQuery(encodedPackage, packageSequence);
				break;
				
			case NetworkConst.REQUEST_QUERY_TIME:
				handleTimeQuery(encodedPackage, packageSequence);
				break;
	
			default:
				logger.warn("Unsupported query type: " + queryType);
				writeResultPackage(new ErrorResponse(packageSequence));
				return true;
		}

		return true;
	}
	
	/**
	 * Handle the transfer package. In contrast to other packages, this package
	 * type can become very large. Therefore, the data is not buffered into a byte 
	 * buffer. The network stream is directly passed to the decoder.
	 * 
	 * @param bb
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleTransfer(final ByteBuffer packageHeader, final short packageSequence) {
		
		final long bodyLength = NetworkPackageDecoder.getBodyLengthFromRequestPackage(packageHeader);
		final ScalephantConfiguration configuration = ScalephantConfigurationManager.getConfiguration();
		
		try {
			TransferSSTableRequest.decodeTuple(packageHeader, bodyLength, configuration, inputStream);
		} catch (IOException e) {
			logger.warn("Exception while handling sstable transfer", e);
		}
		
		return true;
	}
	
	/**
	 * Create a new distribution group
	 * @param packageHeader
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleCreateDistributionGroup(final ByteBuffer packageHeader, final short packageSequence) {
		final ByteBuffer encodedPackage = readFullPackage(packageHeader);
		
		final CreateDistributionGroupRequest createPackage = CreateDistributionGroupRequest.decodeTuple(encodedPackage);
		logger.info("Create distribution group: " + createPackage.getDistributionGroup());
		
		try {
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			zookeeperClient.createDistributionGroup(createPackage.getDistributionGroup(), createPackage.getReplicationFactor());
			
			final DistributionRegion region = zookeeperClient.readDistributionGroup(createPackage.getDistributionGroup());
			
			final ScalephantConfiguration scalephantConfiguration = 
					ScalephantConfigurationManager.getConfiguration();

			final String localIp = scalephantConfiguration.getLocalip();
			final int localPort = scalephantConfiguration.getNetworkListenPort();
			
			final DistributedInstance intance = new DistributedInstance(localIp, localPort, Const.VERSION);
			zookeeperClient.addSystemToDistributionRegion(region, intance);
			
			writeResultPackage(new SuccessResponse(packageSequence));
		} catch (Exception e) {
			logger.warn("Error while create distribution group", e);
			writeResultPackage(new ErrorResponse(packageSequence));	
		}
		
		return true;
	}
	
	/**
	 * Delete an existing distribution group
	 * @param packageHeader
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleDeleteDistributionGroup(final ByteBuffer packageHeader, final short packageSequence) {
		final ByteBuffer encodedPackage = readFullPackage(packageHeader);
		
		final DeleteDistributionGroupRequest deletePackage = DeleteDistributionGroupRequest.decodeTuple(encodedPackage);
		logger.info("Delete distribution group: " + deletePackage.getDistributionGroup());
		
		try {
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			zookeeperClient.deleteDistributionGroup(deletePackage.getDistributionGroup());
			writeResultPackage(new SuccessResponse(packageSequence));
		} catch (Exception e) {
			logger.warn("Error while delete distribution group", e);
			writeResultPackage(new ErrorResponse(packageSequence));	
		}
		
		return true;
	}

	/**
	 * Handle a key query
	 * @param encodedPackage
	 * @param packageSequence
	 */
	protected void handleKeyQuery(final ByteBuffer encodedPackage,
			final short packageSequence) {
		
		final QueryKeyRequest queryKeyRequest = QueryKeyRequest.decodeTuple(encodedPackage);
		final String table = queryKeyRequest.getTable().getFullname();
		
		try {
			final StorageManager storageManager = StorageInterface.getStorageManager(table);

			final Tuple tuple = storageManager.get(queryKeyRequest.getKey());
			
			if(tuple != null) {
				writeResultPackage(new TupleResponse(packageSequence, table, tuple));
			} else {
				writeResultPackage(new SuccessResponse(packageSequence));
			}
			
			return;
			
		} catch (StorageManagerException e) {
			logger.warn("Got exception while scanning for key", e);
		}
		
		writeResultPackage(new ErrorResponse(packageSequence));
	}
	
	/**
	 * Handle a bounding box query
	 * @param encodedPackage
	 * @param packageSequence
	 */
	protected void handleBoundingBoxQuery(final ByteBuffer encodedPackage,
			final short packageSequence) {
		
		final QueryBoundingBoxRequest queryRequest = QueryBoundingBoxRequest.decodeTuple(encodedPackage);
		final String table = queryRequest.getTable().getFullname();
		
		try {
			final StorageManager storageManager = StorageInterface.getStorageManager(table);

			final Collection<Tuple> resultTuple = storageManager.getTuplesInside(queryRequest.getBoundingBox());
			
			writeResultPackage(new MultipleTupleStartResponse(packageSequence));
			
			for(final Tuple tuple : resultTuple) {
				writeResultPackage(new TupleResponse(packageSequence, table, tuple));
			}

			writeResultPackage(new MultipleTupleEndResponse(packageSequence));

			return;
		} catch (StorageManagerException e) {
			logger.warn("Got exception while scanning for bbox", e);
		}
		
		writeResultPackage(new ErrorResponse(packageSequence));
	}
	
	/**
	 * Handle a time query
	 * @param encodedPackage
	 * @param packageSequence
	 */
	protected void handleTimeQuery(final ByteBuffer encodedPackage,
			final short packageSequence) {
		
		final QueryTimeRequest queryRequest = QueryTimeRequest.decodeTuple(encodedPackage);
		final String table = queryRequest.getTable().getFullname();
		
		try {
			final StorageManager storageManager = StorageInterface.getStorageManager(table);

			final Collection<Tuple> resultTuple = storageManager.getTuplesAfterTime(queryRequest.getTimestamp());
			
			writeResultPackage(new MultipleTupleStartResponse(packageSequence));
			
			for(final Tuple tuple : resultTuple) {
				writeResultPackage(new TupleResponse(packageSequence, table, tuple));
			}
			
			writeResultPackage(new MultipleTupleEndResponse(packageSequence));
			
			return;
		} catch (StorageManagerException e) {
			logger.warn("Got exception while scanning for time", e);
		}
		
		writeResultPackage(new ErrorResponse(packageSequence));
	}

	/**
	 * Handle Insert tuple package
	 * @param bb
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleInsertTuple(final ByteBuffer packageHeader, final short packageSequence) {
		final ByteBuffer encodedPackage = readFullPackage(packageHeader);

		final InsertTupleRequest insertTupleRequest = InsertTupleRequest.decodeTuple(encodedPackage);
		
		// Propergate the call to the storage manager
		final Tuple tuple = insertTupleRequest.getTuple();
		
		try {
			final StorageManager table = StorageInterface.getStorageManager(insertTupleRequest.getTable().getFullname());
			table.put(tuple);
			writeResultPackage(new SuccessResponse(packageSequence));
		} catch (StorageManagerException e) {
			logger.warn("Error while insert tuple", e);
			writeResultPackage(new ErrorResponse(packageSequence));	
		}
		
		return true;
	}

	/**
	 * Handle list tables package
	 * @param bb
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleListTables(final ByteBuffer packageHeader, final short packageSequence) {
		readFullPackage(packageHeader);
		final List<String> allTables = StorageInterface.getAllTables();
		final ListTablesResponse listTablesResponse = new ListTablesResponse(packageSequence, allTables);
		writeResultPackage(listTablesResponse);
		
		return true;
	}

	/**
	 * Handle delete tuple package
	 * @param bb
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleDeleteTuple(final ByteBuffer packageHeader, final short packageSequence) {

		final ByteBuffer encodedPackage = readFullPackage(packageHeader);

		final DeleteTupleRequest deleteTupleRequest = DeleteTupleRequest.decodeTuple(encodedPackage);
		
		writeResultPackage(new SuccessResponse(packageSequence));

		// Propergate the call to the storage manager
		try {
			final StorageManager table = StorageInterface.getStorageManager(deleteTupleRequest.getTable().getFullname());
			table.delete(deleteTupleRequest.getKey());
			writeResultPackage(new SuccessResponse(packageSequence));
		} catch (StorageManagerException e) {
			logger.warn("Error while delete tuple", e);
			writeResultPackage(new ErrorResponse(packageSequence));	
		}
		
		return true;
	}	

	/**
	 * Read the full package. The total length of the package is read from the package header.
	 * @param packageHeader
	 * @return
	 */
	protected ByteBuffer readFullPackage(final ByteBuffer packageHeader) {
		final int bodyLength = (int) NetworkPackageDecoder.getBodyLengthFromRequestPackage(packageHeader);
		final ByteBuffer encodedPackage = ByteBuffer.allocate(packageHeader.limit() + bodyLength);
		encodedPackage.put(packageHeader.array());
		
		try {
			//System.out.println("Trying to read: " + bodyLength + " avail " + in.available());
			inputStream.read(encodedPackage.array(), encodedPackage.position(), bodyLength);
		} catch (IOException e) {
			logger.error("IO-Exception while reading package", e);
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
			return null;
		}
		
		return encodedPackage;
	}

	/**
	 * Handle the next request package
	 * @throws IOException
	 */
	protected void handleNextPackage() throws IOException {
		final ByteBuffer packageHeader = readNextPackageHeader();

		final short packageSequence = NetworkPackageDecoder.getRequestIDFromRequestPackage(packageHeader);
		final byte packageType = NetworkPackageDecoder.getPackageTypeFromRequest(packageHeader);
		
		boolean readFurtherPackages = true;
		
		switch (packageType) {
			case NetworkConst.REQUEST_TYPE_DISCONNECT:
				logger.info("Got disconnect package, preparing for connection close: "  + clientSocket.getInetAddress());
				writeResultPackage(new SuccessResponse(packageSequence));
				readFurtherPackages = false;
				break;
				
			case NetworkConst.REQUEST_TYPE_DELETE_TABLE:
				if(logger.isDebugEnabled()) {
					logger.debug("Got delete table package");
				}
				readFurtherPackages = handleDeleteTable(packageHeader, packageSequence);
				break;
				
			case NetworkConst.REQUEST_TYPE_DELETE_TUPLE:
				if(logger.isDebugEnabled()) {
					logger.debug("Got delete tuple package");
				}
				readFurtherPackages = handleDeleteTuple(packageHeader, packageSequence);
				break;
				
			case NetworkConst.REQUEST_TYPE_LIST_TABLES:
				if(logger.isDebugEnabled()) {
					logger.debug("Got list tables request");
				}
				readFurtherPackages = handleListTables(packageHeader, packageSequence);
				break;
				
			case NetworkConst.REQUEST_TYPE_INSERT_TUPLE:
				if(logger.isDebugEnabled()) {
					logger.debug("Got insert tuple request");
				}
				readFurtherPackages = handleInsertTuple(packageHeader, packageSequence);
				break;
				
			case NetworkConst.REQUEST_TYPE_QUERY:
				if(logger.isDebugEnabled()) {
					logger.debug("Got query package");
				}
				readFurtherPackages = handleQuery(packageHeader, packageSequence);
				break;

			case NetworkConst.REQUEST_TYPE_TRANSFER:
				if(logger.isDebugEnabled()) {
					logger.debug("Got transfer package");
				}
				readFurtherPackages = handleTransfer(packageHeader, packageSequence);
				break;
				
			case NetworkConst.REQUEST_TYPE_CREATE_DISTRIBUTION_GROUP:
				if(logger.isDebugEnabled()) {
					logger.debug("Got create distribution group package");
				}
				readFurtherPackages = handleCreateDistributionGroup(packageHeader, packageSequence);
				break;
		
			case NetworkConst.REQUEST_TYPE_DELETE_DISTRIBUTION_GROUP:
				if(logger.isDebugEnabled()) {
					logger.debug("Got delete distribution group package");
				}
				readFurtherPackages = handleDeleteDistributionGroup(packageHeader, packageSequence);
				break;
				
			default:
				logger.warn("Got unknown package type, closing connection: " + packageType);
				connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
				break;
		}
		
		if(readFurtherPackages == false) {
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
		}	
	}
}