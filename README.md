# ReadMe

This application is a playground for various time-related explorations with Apache Flink.

# Setup

For most of the services that are needed, you can use docker:

```cd docker && docker-compose up -d```

This will start influxdb, grafana, and prometheus. 
It will take about one minute until the datasource and dashboard are added to Grafana.

Grafana is served on `localhost:3000`. Login as admin/admin.

The Prometheus UI is served on `localhost:9090`.

You will also need a local Flink cluster, which is not included.

## Note for linux users

The prometheus configuration works out of the box for Mac and Windows, but on Linux you will need to edit `docker/prometheus/prometheus.yml`
before starting docker, and change both instances of `host.docker.internal` to `172.17.0.1`.

# Disclaimer
Apache®, Apache Flink™, Flink™, and the Apache feather logo are trademarks of [The Apache Software Foundation](http://apache.org).
