---
layout: page
title: "User defined filter"
category: doc
date: 2016-12-12 12:18:27
order: 3
---

This page describe the _user defined filter_ feature that is available beginning with BBoxDB 0.9.2. User defined filter allow it to filter a result (e.g., the result of a range query or a join) with a further filter. This filter has to be created by a user and can decode the real meaning of a value. Normally, BBoxDB can only execute operation based on the bounding boxes of the tuples. The user defined filter are executed directly on each BBoxDB node before the result is send back to the client.

### Creating a user defined filter

To define a new user defined filter, the interface `UserDefinedFilter` has to be implemented. In the methods `filterTuple(Tuple, String)` and `filterTuple(Tuple, Tuple, String)` has the filter code to be implemented.

The first method is called when a single tuple needs to be filtered. In addition to the tuple, a user defined value is passed to the method. The value is determined by the user which executed the query. The value can be used to change the behavior of the filter (e.g., let only tuple pass that contain this value) Depending on the method return value (`true` or `false`) the tuple is send back to the client or nor.

The second method is executed, when two tuples needs to be filtered (e.g., in a join operation). Like in the first filter operation, also a user defined value is passed to the method call.

### Example: A user defined filter that filters strings

In the following example, a user defined filter is defined. This filter filters the tuple based on the occurrence of a certain string in the tuple value. The tuple value is passed by the user defined value.

```java
package org.bboxdb.network.query.filter;

import org.bboxdb.storage.entity.Tuple;

public class UserDefinedStringFilter implements UserDefinedFilter {

	@Override
	public boolean filterTuple(final Tuple tuple, final String customData) {
		return new String(tuple.getDataBytes()).contains(customData);
	}

	@Override
	public boolean filterJoinCandidate(final Tuple tuple1, final Tuple tuple2, final String customData) {
		final String string1 = new String(tuple1.getDataBytes());
		final String string2 = new String(tuple2.getDataBytes());

		return string1.contains(customData) && string2.contains(customData);
	}
}
```