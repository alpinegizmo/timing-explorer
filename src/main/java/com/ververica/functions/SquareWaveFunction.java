package com.ververica.functions;

import com.ververica.data.DataPoint;

import org.apache.flink.api.common.functions.MapFunction;

/*
 * Expects a sawtooth wave as input!
 */
public class SquareWaveFunction implements MapFunction<DataPoint<Double>, DataPoint<Double>> {
  @Override
  public DataPoint<Double> map(DataPoint<Double> dataPoint) throws Exception {
    double value = 0.0;
    if(dataPoint.getValue() > 0.4){
      value = 1.0;
    }
    return dataPoint.withNewValue(value);
  }
}
