/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.experiments;

import java.io.File;
import java.io.IOException;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.FileLineIndex;
import org.bboxdb.tools.converter.tuple.ADSBTupleBuilder2D;
import org.bboxdb.tools.converter.tuple.ADSBTupleBuilder3D;
import org.bboxdb.tools.converter.tuple.AuTransportGeoJSONTupleBuilder;
import org.bboxdb.tools.converter.tuple.BerlinModTupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilder;

public class AbstractStateSize {

	/**
	 * The input file
	 */
	protected final File inputFile;
	
	/**
	 * The tuple factory
	 */
	protected final TupleBuilder tupleBuilder;
	
	/**
	 * The time the lastWatermark is generated
	 */
	protected long lastWatermarkGenerated;
	
	/**
	 * The debug flag
	 */
	protected boolean DEBUG = false;

	public AbstractStateSize(File inputFile, TupleBuilder tupleFactory) {
		this.inputFile = inputFile;
		this.tupleBuilder = tupleFactory;
		this.lastWatermarkGenerated = 0;
	}

	/**
	 * Is a watermark generated
	 * @param lastTuple
	 * @param tuple
	 * @return
	 */
	protected boolean isWatermarkCreated(final Tuple lastTuple, final Tuple tuple) {
		
		if(lastTuple == null) {
			return false;
		}
		
	
		if(tupleBuilder instanceof ADSBTupleBuilder2D || tupleBuilder instanceof ADSBTupleBuilder3D) {
			final GeoJsonPolygon polygonOld = GeoJsonPolygon.fromGeoJson(new String(lastTuple.getDataBytes()));			
			final GeoJsonPolygon polygonNew = GeoJsonPolygon.fromGeoJson(new String(tuple.getDataBytes()));			
	
			return (polygonOld.getId() > polygonNew.getId());
		}
		
		if(tupleBuilder instanceof BerlinModTupleBuilder) {
			
			final long newTimestamp = tuple.getVersionTimestamp();
			
			if(lastWatermarkGenerated == 0) {
				lastWatermarkGenerated = newTimestamp;
				return false;
			}
			
			if(lastWatermarkGenerated + 60_000 < newTimestamp) {
				lastWatermarkGenerated = newTimestamp;
				return true;
			}
			
			return false;
		}
		
		if(tupleBuilder instanceof AuTransportGeoJSONTupleBuilder) {
	
			final GeoJsonPolygon polygonNew = GeoJsonPolygon.fromGeoJson(new String(tuple.getDataBytes()));			
			final String newTimestampString = polygonNew.getProperties().getOrDefault("TimestampParsed", "-1");
			final long newTimestamp = MathUtil.tryParseLongOrExit(newTimestampString, () -> "Unable to parse: " + newTimestampString);
			
			if(lastWatermarkGenerated == 0) {
				lastWatermarkGenerated = newTimestamp;
				return false;
			}
			
			if(lastWatermarkGenerated + 60 < newTimestamp) {
				lastWatermarkGenerated = newTimestamp;
				return true;
			}
			
			return false;
		}
		
		System.out.println("Unsupported tuple builder: " + tupleBuilder);
		System.exit(1);
		return false;
	}

	/**
	 * Determine the lines of the file
	 * @return
	 * @throws IOException 
	 */
	protected long determineLinesInInput() {
		final String filename = inputFile.getAbsolutePath();
		
		try(FileLineIndex fli = new FileLineIndex(filename)) {
			System.out.format("Indexing %s%n", filename);
			fli.indexFile();
			return fli.getIndexedLines();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
			return -1;
		}
	}

}
