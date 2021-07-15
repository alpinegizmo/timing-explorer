/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.jobs;

import com.ververica.functions.AssignKeyFunction;
import com.ververica.functions.PseudoWindow;
import com.ververica.functions.SawtoothFunction;
import com.ververica.functions.SensorDataWatermarkAssigner;
import com.ververica.functions.SineWaveFunction;
import com.ververica.sinks.InfluxDBSink;
import com.ververica.sources.TimestampSource;
import com.ververica.data.DataPoint;
import com.ververica.data.KeyedDataPoint;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Paths;

public class TimingExplorer {

  public static void main(String[] args) throws Exception {
    String cwd = Paths.get(".").toAbsolutePath().normalize().toString();
    Configuration conf = GlobalConfiguration.loadConfiguration(cwd);

    final StreamExecutionEnvironment env;
    ParameterTool parameters = ParameterTool.fromArgs(args);

    // Set this to false if you aren't running in an IDE
    final boolean webui = false;

    final boolean useRocksDB = parameters.has("rocksdb");

    if (webui) {
      // Start up the webserver (only for use when run in an IDE)
      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
    } else {
      // Connect to whatever cluster can be found (which may have its own webserver)
      env = StreamExecutionEnvironment.getExecutionEnvironment();
      FileSystem.initialize(conf);
    }

    if (useRocksDB) {
      EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
      backend.isIncrementalCheckpointsEnabled();
      env.setStateBackend(backend);
    } else {
      env.setStateBackend(new HashMapStateBackend());
    }
    env.getCheckpointConfig().setCheckpointStorage("file:///tmp/checkpoints");

    env.enableCheckpointing(1000);
    CheckpointConfig config = env.getCheckpointConfig();
    config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    // Simulate some sensor data
    DataStream<KeyedDataPoint<Double>> sensorStream = generateSensorData(env);

    // Write this sensor stream out to InfluxDB
    sensorStream
            .addSink(new InfluxDBSink<>("sensors"))
            .name("sensors-sink");

    // Compute a windowed sum over this data and write that to InfluxDB as well.
    sensorStream
            .keyBy(p -> p.getKey())
            .process(new PseudoWindow(true, 1000))
            .uid("event-time-window")
            .name("event-time-window")
            .addSink(new InfluxDBSink<>("eventsPerSecond"))
            .name("events-per-second-sink");

    // Compute a windowed sum over this data and write that to InfluxDB as well.
    sensorStream
            .keyBy(p -> p.getKey())
            .process(new PseudoWindow(false, 1000))
            .uid("processing-time-window")
            .name("processing-time-window")
            .addSink(new InfluxDBSink<>("eventsProcessedPerSecond"))
            .name("events-processed-per-second-sink");

    // execute program
    env.execute("Flink Timing Explorer");
  }

  private static DataStream<KeyedDataPoint<Double>> generateSensorData(StreamExecutionEnvironment env) {

    // boilerplate for this demo
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, 1000));
    env.setMaxParallelism(128);
    env.setParallelism(1);
    env.disableOperatorChaining();
    env.getConfig().setLatencyTrackingInterval(1000);

    final int SLOWDOWN_FACTOR = 1;
    final int PERIOD_MS = 100;

    // Initial data - just timestamped messages
    DataStream<DataPoint<Long>> timestampSource = env
            .addSource(new TimestampSource(PERIOD_MS, SLOWDOWN_FACTOR))
            .uid("timestamp-source")
            .name("timestamp-source");

    timestampSource = timestampSource.assignTimestampsAndWatermarks(new SensorDataWatermarkAssigner());

    // Transform into sawtooth pattern
    SingleOutputStreamOperator<DataPoint<Double>> sawtoothStream = timestampSource
            .map(new SawtoothFunction(10))
            .uid("sawTooth")
            .name("sawTooth");

    // Simulate temp sensor
    SingleOutputStreamOperator<KeyedDataPoint<Double>> tempStream = sawtoothStream
            .map(new AssignKeyFunction("temp"))
            .name("assignKey(temp)");

    // Make sine wave and use for pressure sensor
    SingleOutputStreamOperator<KeyedDataPoint<Double>> pressureStream = sawtoothStream
            .map(new SineWaveFunction())
            .name("sineWave")
            .map(new AssignKeyFunction("pressure"))
            .name("assignKey(pressure");

    // Combine all the streams into one
    DataStream<KeyedDataPoint<Double>> sensorStream = tempStream
            .union(pressureStream);

    return sensorStream;
  }

}
