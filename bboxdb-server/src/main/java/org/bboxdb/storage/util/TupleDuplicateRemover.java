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
package org.bboxdb.storage.util;

import java.util.HashSet;
import java.util.Set;

import org.bboxdb.storage.entity.Tuple;

public class TupleDuplicateRemover {

	/**
	 * The seen keys and versions
	 */
	protected Set<KeyAndVersion> seenKeysAndVersions = new HashSet<>();
	
	public boolean isTupleAlreadySeen(final Tuple tuple) {
		final KeyAndVersion keyAndVersion = new KeyAndVersion(tuple.getKey(), tuple.getVersionTimestamp());
		
		if(! seenKeysAndVersions.contains(keyAndVersion)) {
			seenKeysAndVersions.add(keyAndVersion);
			return false;
		}
		
		return true;
	}

	class KeyAndVersion {
		private final String key;
		private final long version;
		
		public KeyAndVersion(final String key, final long version) {
			this.key = key;
			this.version = version;
		}

		public String getKey() {
			return key;
		}

		public long getVersion() {
			return version;
		}

		@Override
		public String toString() {
			return "KeyAndVersion [key=" + key + ", version=" + version + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			result = prime * result + (int) (version ^ (version >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			KeyAndVersion other = (KeyAndVersion) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (key == null) {
				if (other.key != null)
					return false;
			} else if (!key.equals(other.key))
				return false;
			if (version != other.version)
				return false;
			return true;
		}

		private TupleDuplicateRemover getOuterType() {
			return TupleDuplicateRemover.this;
		}
	}
}
