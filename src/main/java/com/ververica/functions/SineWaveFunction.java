package com.ververica.functions;

import com.ververica.data.DataPoint;

import org.apache.flink.api.common.functions.MapFunction;

/*
 * Expects a sawtooth wave as input!
 */
public class SineWaveFunction implements MapFunction<DataPoint<Double>, DataPoint<Double>> {
  @Override
  public DataPoint<Double> map(DataPoint<Double> dataPoint) throws Exception {
    double phase = dataPoint.getValue() * 2 * Math.PI;
    return dataPoint.withNewValue(Math.sin(phase));
  }
}
