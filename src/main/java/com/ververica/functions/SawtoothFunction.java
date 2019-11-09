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
