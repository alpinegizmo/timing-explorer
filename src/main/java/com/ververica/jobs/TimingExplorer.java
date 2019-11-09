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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Paths;

public class TimingExplorer {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env;
    ParameterTool parameters = ParameterTool.fromArgs(args);

    // Set this to true if you are running in an IDE and want to be able to use the web UI
    final boolean webui = false;

    final boolean eventTime = parameters.getBoolean("eventTime", false);
    final boolean useRocksDB = parameters.getBoolean("rocksdb", false);

    if (webui) {
      // Start up the webserver (only for use when run in an IDE)
      Configuration conf = new Configuration();
      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
    } else {
      // Connect to whatever cluster can be found (which may have its own webserver)
      env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    String cwd = Paths.get(".").toAbsolutePath().normalize().toString();
    FileSystem.initialize(GlobalConfiguration.loadConfiguration(cwd));

    // Use event time
    if (eventTime) {
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }

    if (useRocksDB) {
      RocksDBStateBackend backend = new RocksDBStateBackend("file:///tmp/rocksdb-for-timing-explorer");
      env.setStateBackend(backend);
    }

    env.enableCheckpointing(1000);

    // Simulate some sensor data
    DataStream<KeyedDataPoint<Double>> sensorStream = generateSensorData(env);

    // Write this sensor stream out to InfluxDB
    sensorStream
            .addSink(new InfluxDBSink<>("sensors"))
            .name("sensors-sink");

    // Compute a windowed sum over this data and write that to InfluxDB as well.
    sensorStream
            .keyBy(p -> p.getKey())
            .process(new PseudoWindow(eventTime, 1000))
            .uid("window")
            .name("window")
            .addSink(new InfluxDBSink<>("summedSensors"))
            .name("summed-sensors-sink");


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

    if (env.getStreamTimeCharacteristic() == TimeCharacteristic.EventTime) {
      timestampSource = timestampSource.assignTimestampsAndWatermarks(new SensorDataWatermarkAssigner());
    }

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
