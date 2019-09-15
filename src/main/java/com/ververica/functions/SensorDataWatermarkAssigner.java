package com.ververica.functions;

import com.ververica.data.KeyedDataPoint;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class SensorDataWatermarkAssigner implements AssignerWithPeriodicWatermarks<KeyedDataPoint<Double>> {
  transient long lastTS;

  @Nullable
  @Override
  public Watermark getCurrentWatermark() {
    return new Watermark(lastTS);
  }

  @Override
  public long extractTimestamp(KeyedDataPoint<Double> doubleKeyedDataPoint, long l) {
    lastTS = doubleKeyedDataPoint.getTimeStampMs();
    return lastTS;
  }
}
