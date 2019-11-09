package com.ververica.functions;

import com.ververica.data.DataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

public class SawtoothFunction extends RichMapFunction<DataPoint<Long>, DataPoint<Double>> implements CheckpointedFunction {

  final private int numSteps;
  private Counter datapoints;

  // Checkpointed State
  private transient ListState<Integer> checkpointedStep;
  private int currentStep;

  public SawtoothFunction(int numSteps){
    this.numSteps = numSteps;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>("checkpointedStep", Integer.class);

    this.checkpointedStep = context.getOperatorStateStore().getListState(descriptor);

    if (context.isRestored()) {
      for (Integer step : checkpointedStep.get()) {
        this.currentStep = step;
      }
    } else {
      this.currentStep = 0;
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    checkpointedStep.clear();
    checkpointedStep.add(currentStep);
  }

  @Override
  public void open(Configuration config) {
    this.datapoints = getRuntimeContext()
            .getMetricGroup()
            .counter("datapoints");
  }

  @Override
  public DataPoint<Double> map(DataPoint<Long> dataPoint) throws Exception {
    double phase = (double) currentStep / numSteps;
    currentStep = ++currentStep % numSteps;
    this.datapoints.inc();
    return dataPoint.withNewValue(phase);
  }

}
