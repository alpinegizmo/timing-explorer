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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.functions.StateBootstrapFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;

import com.ververica.data.KeyedDataPoint;

import java.util.Map;

public class ChangeBackend {
	static class PseudoWindowReaderFunction extends
			KeyedStateReaderFunction<String, KeyedDataPoint<Integer>> {
		MapState<Long, Integer> countInWindow;

		@Override
		public void open(Configuration parameters) {
			MapStateDescriptor<Long, Integer> countDesc =
					new MapStateDescriptor<>("countInWindow", Long.class, Integer.class);
			countInWindow = getRuntimeContext().getMapState(countDesc);
		}

		@Override
		public void readKey(
				String key,
				Context context,
				Collector<KeyedDataPoint<Integer>> out) throws Exception {

			for (Map.Entry<Long, Integer> entry : countInWindow.entries()) {
				out.collect(new KeyedDataPoint<Integer>(key, entry.getKey(), entry.getValue()));
			}
		}
	}

	private static class TSBootstrapper extends StateBootstrapFunction<Long> {
		private transient ListState<Long> checkpointedTime;

		@Override
		public void processElement(Long value, Context ctx) throws Exception {
			System.out.println("----time value:");
			System.out.println(value);
			checkpointedTime.clear();
			checkpointedTime.add(value);
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			System.out.println("----snapshotState:");
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			System.out.println("----initializeState:");
			ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>(
					"checkpointedTime",
					Long.class);

			this.checkpointedTime = context.getOperatorStateStore().getListState(descriptor);
		}
	}

	private static class StepBootstrapper extends StateBootstrapFunction<Integer> {
		private transient ListState<Integer> step;

		@Override
		public void processElement(Integer value, Context ctx) throws Exception {
			System.out.println("----step value:");
			System.out.println(value);
			step.clear();
			step.add(value);
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			System.out.println("----snapshotState:");
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			System.out.println("----initializeState:");
			ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>(
					"step",
					Integer.class);

			this.step = context.getOperatorStateStore().getListState(descriptor);
		}
	}

	public static class WindowBootstrapper extends KeyedStateBootstrapFunction<String, KeyedDataPoint<Integer>> {
		private MapState<Long, Integer> countInWindow;
		boolean eventTimeProcessing;

		public WindowBootstrapper(boolean eventTime) {
			this.eventTimeProcessing = eventTime;
		}

		@Override
		public void open(Configuration parameters) {
			MapStateDescriptor<Long, Integer> countDesc =
					new MapStateDescriptor<>("countInWindow", Long.class, Integer.class);
			countInWindow = getRuntimeContext().getMapState(countDesc);
		}

		@Override
		public void processElement(KeyedDataPoint<Integer> value, Context ctx) throws Exception {
			TimerService timerService = ctx.timerService();

			System.out.println("----keyed data point into window:");
			System.out.println(value.toString());
			countInWindow.put(value.getTimeStampMs(), value.getValue());

			if (eventTimeProcessing) {
				timerService.registerEventTimeTimer(value.getTimeStampMs());
			} else {
				timerService.registerProcessingTimeTimer(value.getTimeStampMs());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		ParameterTool parameters = ParameterTool.fromArgs(args);
		final boolean eventTime = parameters.getBoolean("eventTime", true);
		final boolean heap = parameters.getBoolean("heap-based-input", true);
		final boolean save = parameters.getBoolean("save", true);

		final String pathToSavepoint = "/Users/david/stuff/timing-explorer/sp/savepoint-a74596-0f026b1cb709";

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		ExistingSavepoint sp;

		if (heap) {
			MemoryStateBackend backend = new MemoryStateBackend();
			sp = Savepoint.load(env, pathToSavepoint, backend);
		} else {
			RocksDBStateBackend backend = new RocksDBStateBackend("file:///tmp/rocksdb-for-reading-savepoint");
			sp = Savepoint.load(env, pathToSavepoint, backend);
		}

		DataSet<Long> currentTimeMs = sp.readListState(
				"timestamp-source", "checkpointedTime", Types.LONG);
		DataSet<Integer> currentStep = sp.readListState(
				"sawTooth", "checkpointedStep", Types.INT);
		DataSet<KeyedDataPoint<Integer>> keyedState = sp.readKeyedState(
				"window", new PseudoWindowReaderFunction());

		System.out.println("----currentTimeMs from timestamp-source:");
		currentTimeMs.print();
		System.out.println("----currentStep from sawTooth:");
		currentStep.print();
		System.out.println("----keyed state from window:");
		keyedState.print();

		if (save) {
			BootstrapTransformation<Long> sourceXform = OperatorTransformation
					.bootstrapWith(currentTimeMs)
					.transform(new TSBootstrapper());

			BootstrapTransformation<Integer> stepXform = OperatorTransformation
					.bootstrapWith(currentStep)
					.transform(new StepBootstrapper());

			BootstrapTransformation<KeyedDataPoint<Integer>> windowXform = OperatorTransformation
					.bootstrapWith(keyedState)
					.keyBy(x -> x.getKey())
					.transform(new WindowBootstrapper(eventTime));

			Savepoint
					.create(new RocksDBStateBackend("file:///tmp/rocksdb-for-writing-savepoint"), 128)
					.withOperator("timestamp-source", sourceXform)
					.withOperator("sawTooth", stepXform)
					.withOperator("window", windowXform)
					.write("/tmp/new-savepoint");
		}

		env.execute("Change state backend");
	}
}
