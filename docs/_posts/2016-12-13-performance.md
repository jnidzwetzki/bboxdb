---
layout: page
title: "Performance counter"
category: doc
date: 2016-12-12 22:46:12
order: 2
---

To optimize the performance of BBoxDB, the software maintaines some performance counter. The counter contains information about the undlushed memtables, read and written bytes to disk or network connections. 

## Capture the performance counter with prometheus
Download and unpack [prometheus](https://prometheus.io)

```bash
wget https://github.com/prometheus/prometheus/releases/download/v2.0.0/prometheus-2.0.0.linux-amd64.tar.gz
tar zxvf prometheus-2.0.0.linux-amd64.tar.gz
cd prometheus-2.0.0.linux-amd64
```
