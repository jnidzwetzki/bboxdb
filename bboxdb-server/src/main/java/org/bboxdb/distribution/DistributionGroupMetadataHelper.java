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
package org.bboxdb.distribution;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.bboxdb.storage.entity.DistributionGroupMetadata;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public class DistributionGroupMetadataHelper {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionGroupMetadataHelper.class);
	
	/**
	 * Write new metadata for a distribution group
	 * @param distributionGroupName
	 * @param distributionGroupMetadata
	 * @throws IOException 
	 */
	public static void writeMedatadataForGroup(final String basedir, 
			final DistributionGroupName distributionGroupName, 
			final DistributionGroupMetadata distributionGroupMetadata) throws IOException {
		
	    final Map<String, Object> data = new HashMap<>();
	    data.put("version", distributionGroupMetadata.getVersion());
	    
	    final String filename = getFilename(basedir, distributionGroupName);
	    final File metadataFile = new File(filename);
	    
	    logger.info("Writing metadata to {} ", metadataFile);
	    
		final FileWriter writer = new FileWriter(metadataFile);
	    
	    final Yaml yaml = new Yaml();
	    yaml.dump(data, writer);
	    writer.close();
	}

	/**
	 * Get the filename of the meta data
	 * @param distributionGroupName
	 * @return
	 */
	protected static String getFilename(final String basedir, 
			final DistributionGroupName distributionGroupName) {
		
		return SSTableHelper.getDistributionGroupMedatadaFile(basedir, 
				distributionGroupName.getFullname());
	}

	/**
	 * Read the metadata data of a distribution group
	 * @param distributionGroupName
	 * @return
	 */
	public static DistributionGroupMetadata getMedatadaForGroup
		(final String basedir, final DistributionGroupName distributionGroupName) {
		
		final Yaml yaml = new Yaml(); 
	    final String filename = getFilename(basedir, distributionGroupName);

	    final File file = new File(filename);
	    
	    if(! file.exists()) {
	    	return null;
	    }
	    
		FileReader reader;
		try {
			reader = new FileReader(filename);
		} catch (FileNotFoundException e) {
			logger.warn("Unable to load file: " + filename, e);
			return null;
		}

		return yaml.loadAs(reader, DistributionGroupMetadata.class);
	}
}
