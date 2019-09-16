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

package com.ververica.sinks;

import com.ververica.data.DataPoint;
import com.ververica.data.KeyedDataPoint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

public class InfluxDBSink<T extends DataPoint<? extends Number>> extends RichSinkFunction<T> {

  private transient InfluxDB influxDB = null;
  private static String dataBaseName = "sineWave";
  private static String fieldName = "value";
  private String measurement;

  private transient DescriptiveStatisticsHistogram eventTimeLag;
  private static final int EVENT_TIME_LAG_WINDOW_SIZE = 10_000;

  public InfluxDBSink(String measurement){
    this.measurement = measurement;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    influxDB = InfluxDBFactory.connect("http://localhost:8086", "admin", "admin");
    influxDB.createDatabase(dataBaseName);
    influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);

    eventTimeLag = getRuntimeContext().getMetricGroup().histogram("eventTimeLag",
            new DescriptiveStatisticsHistogram(EVENT_TIME_LAG_WINDOW_SIZE));
  }

  @Override
  public void close() throws Exception {
    super.close();
  }

  @Override
  public void invoke(T dataPoint, Context context) throws Exception {
    Point.Builder builder = Point.measurement(measurement)
            .time(dataPoint.getTimeStampMs(), TimeUnit.MILLISECONDS)
            .addField(fieldName, dataPoint.getValue());

    if (dataPoint instanceof KeyedDataPoint) {
      builder.tag("key", ((KeyedDataPoint) dataPoint).getKey());
    }

    Point p = builder.build();

    influxDB.write(dataBaseName, "autogen", p);

    eventTimeLag.update(System.currentTimeMillis() - dataPoint.getTimeStampMs());
  }
}
