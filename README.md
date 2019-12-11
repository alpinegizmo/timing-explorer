# ReadMe

This application is a playground for various explorations with Apache Flink.

# Setup

For most of the services that are needed, you can use docker:

```cd docker && docker-compose up -d```

This will start influxdb, grafana, and prometheus. 
It will take about one minute until the data source and dashboard are added to Grafana.

Grafana is served on `localhost:3000`. Login as admin/admin.

The Prometheus UI is served on `localhost:9090`.

You will also need a local Flink cluster, which is not included, or you can just run this app in 
your IDE, which will supply the missing pieces of Flink.

To shut things down, use

```cd docker && docker-compose down```

## Note for linux users

The prometheus configuration works out of the box for Mac and Windows, but on Linux you will need to 
edit `docker/prometheus/prometheus.yml` before starting docker, and change both instances of 
`host.docker.internal` to `172.17.0.1`.

# Explorations

## Event Time vs. Processing Time

The `TimingExplorer` Flink application includes a custom source that generates 10 events per second for
a simulated pressure sensor. These events are sent to an InfluxDB sink that is connected to Grafana.
If you wait for the Grafana dashboard to become available, and start the `TimingExplorer`
job, you will see something like this:

![](images/pressure-graph.png)

This application adds up the events in one-second-long windows, and displays this sum in the dashboard:

![](images/pressure-window.png)

This lower graph of the windowed results would be perfectly flat if each one-second-long window contained 
exactly the 10 events generated each second. However, because the windowing is being done with processing 
time semantics, each window contains however many events happen to be available, whether that's 8, or 11, or
whatever, which leads to the non-deterministic results shown above.

If you rerun the application with `--eventTime true`, then it will produce deterministic results:

![](images/pressure-window-event-time.png)

## Measuring Latency

To explore the metrics you will need to start a real Flink cluster, rather than relying on the
mini cluster that runs in your IDE.

This job has latency tracking enabled, so you can observe the latency tracking metrics. The docker setup
includes an instance of prometheus at `localhost:9090` that you can use to inspect these metrics. Look at
`flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency` to see these values.
To drill in on one operator, try something like `flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency{operator_id="79aa417dc3f201567753101d8995b0b8"}`

![](images/latency-tracking.png)

Also, the `InfluxDBSink` used by this application has a custom histogram metric called _eventTimeLag_ 
that measures the distribution of latencies from source to sink. You can query 
`flink_taskmanager_job_task_operator_eventTimeLag` in the prometheus UI to see these values.

![](images/event-time-lag.png)

### Affecting latency

You can try making a change to the job that should make latency dramatically worse, and see 
how that shows up in these measurements.

## State Backends

This project includes an example of how to switch from one state backend to another using the
State Processor API.

`ChangeBackend` is another Flink application in this project. It can read a savepoint from the 
`TimingExplorer` app made with a heap-based state backend, and write
out a new savepoint that uses the RocksDB state backend. 

You can then run `TimingExplorer` 
with `--rocksdb true` to restart the application using that new savepoint.

# Disclaimer
Apache®, Apache Flink™, Flink™, and the Apache feather logo are trademarks of [The Apache Software Foundation](http://apache.org).
