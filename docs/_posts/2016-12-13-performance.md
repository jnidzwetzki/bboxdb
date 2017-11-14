---
layout: page
title: "Performance counter"
category: doc
date: 2016-12-12 22:46:12
order: 2
---

To optimize the performance of BBoxDB, the software maintaines some performance counter. The counter contains information about the unflushed memtables, read and written bytes to disk or network connections. 

## Enabling performance counter in BBoxDB


## Capture the performance counter with prometheus
Download and unpack [prometheus](https://prometheus.io). Prometheus is used as the data store for the performance counter. 

```bash
wget https://github.com/prometheus/prometheus/releases/download/v2.0.0/prometheus-2.0.0.linux-amd64.tar.gz
tar zxvf prometheus-2.0.0.linux-amd64.tar.gz
cd prometheus-2.0.0.linux-amd64
```

## Visualize the data with Grafana
<img src="/bboxdb/images/grafana_dashboard.jpg" width="800">

```bash
wget https://s3-us-west-2.amazonaws.com/grafana-releases/release/grafana-4.3.2.linux-x64.tar.gz
tar zxvf grafana-4.3.2.linux-x64.tar.gz
cd grafana-4.3.2.linux
./grafana-server
``` 