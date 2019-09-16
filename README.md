# ReadMe

This application is a playground for experimenting with event time vs processing time, 
latency tracking metrics, and other time-related explorations in Flink.

# Setup

A local Flink cluster is needed (and not included in the docker setup described below).

For this Flink application a local Grafana and InfluxDB instance are also needed. 
These can be setup (including basic grafana configuration) via 

```cd docker && docker-compose up -d```

It will take about one minute until the datasource and dashboard are added to Grafana. 
Grafana is served on `localhost:3000`. Login as admin/admin.

Prometheus is also included in the docker-compose setup, and its UI is served on `localhost:9090`. 

## Note for linux users

The prometheus configuration works out of the box for Mac and Windows, but on Linux you will need to edit `docker/prometheus/prometheus.yml`
before starting docker, and change both instances of `host.docker.internal` to `172.17.0.1`.

# Disclaimer
Apache®, Apache Flink™, Flink™, and the Apache feather logo are trademarks of [The Apache Software Foundation](http://apache.org).
