---
layout: page
title: "Continuous queries"
category: doc
date: 2016-12-12 22:46:12
order: 1
---

# Continuous queries
Since version 0.8.6 BBoxDB supports continuous queries. This means that queries can be registered. Every time new data is inserted into one table, the queries are evaluated and matching tuples are reported.
