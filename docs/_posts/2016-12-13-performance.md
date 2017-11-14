---
layout: page
title: "Performance counter"
category: doc
date: 2016-12-12 22:46:12
order: 2
---

To optimize the performance of BBoxDB, the software maintaines some performance counter. The counter contains information about the unflushed memtables, read and written bytes to disk or network connections. 

## Enabling performance counter in BBoxDB
First of all, to work with performance counter, these counter needs to be enabled in BBoxDB. The performance counter is published via HTTP on a specific port. The port is controlled via the ``performanceCounterPort`` variable in the `bboxdb.yaml`` configuration file. The default setting is port 10085; a value of -1 disables the HTTP service. 

After the server was activated, you can open the URL ``http://node-ip:10085/`` in your browser. A text page with some counter should be displayed. You will find counter such as ``bboxdb_read_tuples_bytes`` or ``bboxdb_read_tuple_keys_total`` on the page. Also, details about the JVM are shown. All BBoxDB related counters are starting with the prefix ``bboxdb``.

## Capture the performance counter with prometheus
Download and unpack [prometheus](https://prometheus.io). Prometheus is used as the data store for the performance counter. 

```bash
wget https://github.com/prometheus/prometheus/releases/download/v2.0.0/prometheus-2.0.0.linux-amd64.tar.gz
tar zxvf prometheus-2.0.0.linux-amd64.tar.gz
cd prometheus-2.0.0.linux-amd64
./prometheus --config.file=prometheus.yml
```

## Visualize the data with Grafana
<img src="/bboxdb/images/grafana_dashboard.jpg" width="800">

```bash
wget https://s3-us-west-2.amazonaws.com/grafana-releases/release/grafana-4.3.2.linux-x64.tar.gz
tar zxvf grafana-4.3.2.linux-x64.tar.gz
cd grafana-4.3.2.linux
./grafana-server
``` 