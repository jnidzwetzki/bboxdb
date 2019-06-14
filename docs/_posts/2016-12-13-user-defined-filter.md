---
layout: page
title: "User defined filter"
category: doc
date: 2016-12-12 12:18:27
order: 3
---

This page describes the _user defined filter_ (UDF) feature that is available since BBoxDB 0.9.0. A UDF allow the user to filter a result (e.g., the result of a range query or a join) with a custom filter. This filter has to be created by the user, the user can give the filter the ability to decode and deal with the real meaning of a value. Normally, BBoxDB can only execute operation based on the bounding boxes of the tuples. The user defined filter is executed directly on each BBoxDB node before the result is sent back to the client.

### Creating a user defined filter

To define a new user defined filter, the interface `UserDefinedFilter` has to be implemented. In the methods `filterTuple(Tuple, byte[])` and `filterTuple(Tuple, Tuple, byte[])` has the filter code to be implemented.

The first method is called when a single tuple needs to be filtered. In addition to the tuple, a user defined value is passed to the method. The value is determined by the user which executed the query. The value can be used to change the behavior of the filter (e.g., let only tuple pass that contains this value) Depending on the method return value (`true` or `false`) the tuple is sent back to the client or not.

The second method is executed, when two tuples need to be filtered (e.g., in a join operation). Like in the first filter operation, also a user defined value is passed to the method call.

The filter has to be compiled and embedded in a custom .jar file. To work with this filter, every node has to load this filter into its class path. To accomplish this, the created .jar file has to be placed in the directory `$BBOXDB_HOME/bin/third_party_libs/`. The content of this directory is synchronized across the whole cluster on every `$BBOXDB_HOME/bin/manage_cluster.sh bboxdb_upgrade` call.

### Example: A user defined filter that filters strings

In the following example, a user defined filter is defined. This filter filters the tuple based on the occurrence of a certain string in the tuple value. The tuple value is passed by the user defined value.

```java
package org.bboxdb.network.query.filter;

import org.bboxdb.storage.entity.Tuple;

public class UserDefinedStringFilter implements UserDefinedFilter {

    @Override
    public boolean filterTuple(final Tuple tuple, final byte[] customData) {
        return new String(tuple.getDataBytes()).contains(new String(customData));
    }

    @Override
    public boolean filterJoinCandidate(final Tuple tuple1, final Tuple tuple2, final byte[] customData) {
        final String string1 = new String(tuple1.getDataBytes());
        final String string2 = new String(tuple2.getDataBytes());

        return string1.contains(customData) && string2.contains(new String(customData));
    }
}
```

The filter is defined in the class `org.bboxdb.network.query.filter.UserDefinedStringFilter` and can be used in range query operations or in join operations. 

The following CLI call searches for all tuples that have a bounding box that intersects the space 1:4:1:4 and a value that contains the string `red`.

```bash
$ $BBOXDB_HOME/bin/cli.sh -action query -table mydgroup_table1 -bbox 1:4:1:4 -filter org.bboxdb.network.query.filter.UserDefinedStringFilter -filtervalue red
```

The following CLI command executes a join on the tables `mydgroup_table1` and `mydgroup_table2`. All tuples that have intersecting bounding boxes in the area `0:10:0:8` are passed to the user defined filter. The tuples that pass our user defined filter are returned to the client.

```bash
 $ $BBOXDB_HOME/bin/cli.sh -action join -table mydgroup_table1:mydgroup_table2 -bbox 0:10:0:8 -filter org.bboxdb.network.query.filter.UserDefinedStringFilter -filtervalue red
 ```
 