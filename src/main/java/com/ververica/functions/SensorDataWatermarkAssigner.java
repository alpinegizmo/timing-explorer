package com.ververica.functions;

import com.ververica.data.DataPoint;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class SensorDataWatermarkAssigner implements AssignerWithPeriodicWatermarks<DataPoint<Long>> {
  transient long lastTS;

  @Nullable
  @Override
  public Watermark getCurrentWatermark() {
    return new Watermark(this.lastTS);
  }

  @Override
  public long extractTimestamp(DataPoint<Long> doubleKeyedDataPoint, long l) {
    this.lastTS = doubleKeyedDataPoint.getTimeStampMs();
    return this.lastTS;
  }
}
