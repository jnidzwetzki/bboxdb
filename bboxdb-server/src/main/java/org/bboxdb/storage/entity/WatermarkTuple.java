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
package org.bboxdb.storage.entity;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.sstable.SSTableConst;

public class WatermarkTuple extends Tuple {

	public WatermarkTuple() {
		super(SSTableConst.WATERMARK_KEY, Hyperrectangle.FULL_SPACE, 
				SSTableConst.WATERMARK_MARKER);
	}
	
	public WatermarkTuple(final String key, final long versionTimestamp) {
		super(key, Hyperrectangle.FULL_SPACE, 
				SSTableConst.WATERMARK_MARKER, versionTimestamp);
	}
	
	public WatermarkTuple(final long versionTimestamp) {
		super(SSTableConst.WATERMARK_KEY, Hyperrectangle.FULL_SPACE, 
				SSTableConst.WATERMARK_MARKER, versionTimestamp);
	}

	@Override
	public String toString() {
		return "WatermarkTuple [key=" + key + ", versionTimestamp=" + versionTimestamp + "]";
	}
	
	@Override
	public byte[] getDataBytes() {
		return SSTableConst.WATERMARK_MARKER;
	}
	
	@Override
	public byte[] getBoundingBoxBytes() {
		return SSTableConst.WATERMARK_MARKER;
	}
	
	@Override
	public boolean isPersistentTuple() {
		return false;
	}
}
