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
package org.bboxdb.distribution.partitioner.regionsplit.tuplesink;

import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;

public class NetworkTupleSink extends AbstractTupleSink {
	/**
	 * The connection to spread data too
	 */
	protected final BBoxDBClient connection;

	public NetworkTupleSink(final TupleStoreName tablename, final BBoxDBClient connection) {
		super(tablename);
		this.connection = connection;
	}

	@Override
	public void sinkTuple(final Tuple tuple) throws BBoxDBException {
		sinkedTuples++;
		connection.insertTuple(tablename, tuple);
	}
}
