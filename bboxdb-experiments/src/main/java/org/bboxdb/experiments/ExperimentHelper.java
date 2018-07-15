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
package org.bboxdb.experiments;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.tools.TupleFileReader;

public class ExperimentHelper {

	/**
	 * Determine global bounding box
	 * @param sampleSize
	 * @return
	 * @throws IOException
	 */
	public static Hyperrectangle determineBoundingBox(final String filename, final String format) {
		System.out.println("# Determining the bounding box");

		final TupleFileReader tupleFile = new TupleFileReader(filename, format);
		final List<Hyperrectangle> bboxes = new ArrayList<>();

		tupleFile.addTupleListener(t -> {
			bboxes.add(t.getBoundingBox());
		});

		try {
			tupleFile.processFile();
		} catch (Exception e) {
			System.err.println("Got an Exception during experiment: "+ e);
			System.exit(-1);
		}

		return Hyperrectangle.getCoveringBox(bboxes);
	}

}
