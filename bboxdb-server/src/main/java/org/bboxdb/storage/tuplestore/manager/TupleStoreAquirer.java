/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
package org.bboxdb.storage.tuplestore.manager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.ExceptionHelper;
import org.bboxdb.commons.Retryer;
import org.bboxdb.commons.Retryer.RetryMode;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.misc.Const;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;

public class TupleStoreAquirer implements AutoCloseable {

	private List<ReadOnlyTupleStore> tupleStores;
	
	private final TupleStoreManager tupleStoreManager;
	
	private boolean isReleased = false;
	
	public TupleStoreAquirer(final TupleStoreManager tupleStoreManager) throws StorageManagerException {
		this.tupleStoreManager = tupleStoreManager;
		aquireStorage();
	}

	/**
	 * Try to acquire all needed tables
	 * @return
	 * @throws StorageManagerException
	 */
	private void aquireStorage() throws StorageManagerException {

		final Callable<List<ReadOnlyTupleStore>> callable = getTupleStoreAllocatorCallable();
		
		final Retryer<List<ReadOnlyTupleStore>> retryer = new Retryer<>(Const.OPERATION_RETRY, 
				20, TimeUnit.MILLISECONDS, callable, RetryMode.LINEAR);
		
		try {
			final boolean result = retryer.execute();
			
			if(! result) {
				final List<Exception> exceptions = retryer.getAllExceptions();
				throw new StorageManagerException("Unable to aquire all sstables in "
						+ Const.OPERATION_RETRY + " retries.\n " 
						+ ExceptionHelper.getFormatedStacktrace(exceptions));
			}
			
			tupleStores = retryer.getResult();
			isReleased = false;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new StorageManagerException("Wait for storage was interrupted", e);
		}
	}

	/**
	 * Get the tuple store allocator callable
	 * @return
	 */
	private Callable<List<ReadOnlyTupleStore>> getTupleStoreAllocatorCallable() {
		
		return () -> {
			final List<ReadOnlyTupleStore> aquiredStorages = new ArrayList<>();
			final List<ReadOnlyTupleStore> knownStorages = tupleStoreManager.getAllTupleStorages();

			for(final ReadOnlyTupleStore tupleStorage : knownStorages) {
				final boolean canBeUsed = tupleStorage.acquire();

				if(! canBeUsed) {
					// one or more storages could not be acquired
					// release storages and retry
					
					close();
					
					throw new BBoxDBException("The storage: " + tupleStorage.getInternalName() 
						+ " can't be aquired");					
				} else {
					aquiredStorages.add(tupleStorage);
				}
			}

			return aquiredStorages;
		};		
	}

	/**
	 * Release all acquired tables
	 */
	@Override
	public synchronized void close() {
		if(tupleStores == null) {
			return;
		}
		
		if(! isReleased) {
			tupleStores.forEach(s -> s.release());
		}
		
		isReleased = true;
	}
	
	/**
	 * Get all aquired tuple stores
	 * @return
	 */
	public List<ReadOnlyTupleStore> getTupleStores() {
		return tupleStores;
	}

}
