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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.ververica.data.KeyedDataPoint;

/**
 * For each sensor (the keys), compute the sum of its values over windows that are durationMsec long.
 */

public class PseudoWindow extends KeyedProcessFunction<String, KeyedDataPoint<Double>, KeyedDataPoint<Integer>> {
	// Keyed, managed state, with an entry for each window.
	// There is a separate MapState object for each sensor.
	private MapState<Long, Integer> countInWindow;

	boolean eventTimeProcessing;
	int durationMsec;

	/**
	 * Create the KeyedProcessFunction.
	 * @param eventTime whether or not to use event time processing
	 * @param durationMsec window length
	 */
	public PseudoWindow(boolean eventTime, int durationMsec) {
		this.eventTimeProcessing = eventTime;
		this.durationMsec = durationMsec;
	}

	@Override
	public void open(Configuration config) {
		MapStateDescriptor<Long, Integer> countDesc =
				new MapStateDescriptor<>("countInWindow", Long.class, Integer.class);
		countInWindow = getRuntimeContext().getMapState(countDesc);
	}

	@Override
	public void processElement(
			KeyedDataPoint<Double> dataPoint,
			Context ctx,
			Collector<KeyedDataPoint<Integer>> out) throws Exception {

		long endOfWindow = setTimer(dataPoint, ctx.timerService());

		Integer count = countInWindow.get(endOfWindow);
		if (count == null) {
			count = 0;
		}
		count += 1;
		countInWindow.put(endOfWindow, count);
	}

	public long setTimer(KeyedDataPoint<Double> dataPoint, TimerService timerService) {
		long time;

		if (eventTimeProcessing) {
			time = dataPoint.getTimeStampMs();
		} else {
			time = System.currentTimeMillis();
		}
		long endOfWindow = (time - (time % durationMsec) + durationMsec - 1);

		if (eventTimeProcessing) {
			timerService.registerEventTimeTimer(endOfWindow);
		} else {
			timerService.registerProcessingTimeTimer(endOfWindow);
		}
		return endOfWindow;
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext context, Collector<KeyedDataPoint<Integer>> out) throws Exception {
		// Get the timestamp for this timer and use it to look up the count for that window
		long ts = context.timestamp();
		KeyedDataPoint<Integer> result = new KeyedDataPoint<>(context.getCurrentKey(), ts, countInWindow.get(ts));
		out.collect(result);
		countInWindow.remove(timestamp);
	}
}
