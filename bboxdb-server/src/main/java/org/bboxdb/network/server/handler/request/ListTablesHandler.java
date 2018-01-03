/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
package org.bboxdb.network.server.handler.request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.response.ListTablesResponse;
import org.bboxdb.network.server.ClientConnectionHandler;
import org.bboxdb.storage.entity.TupleStoreName;

public class ListTablesHandler implements RequestHandler {

	@Override
	/**
	 * Handle list tables package
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) 
					throws IOException, PackageEncodeException {

		final List<TupleStoreName> allTables = clientConnectionHandler
				.getStorageRegistry()
				.getAllTables();
		
		final ListTablesResponse listTablesResponse = new ListTablesResponse(packageSequence, allTables);
		clientConnectionHandler.writeResultPackage(listTablesResponse);
		
		return true;
	}
}
