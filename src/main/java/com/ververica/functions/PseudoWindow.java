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

public class PseudoWindow extends KeyedProcessFunction<String, KeyedDataPoint<Double>, KeyedDataPoint<Double>> {
	// Keyed, managed state, with an entry for each window.
	// There is a separate MapState object for each sensor.
	private MapState<Long, Double> sumOfValuesInWindow;

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
		MapStateDescriptor<Long, Double> sumDesc =
				new MapStateDescriptor<>("sumOfValuesInWindow", Long.class, Double.class);
		sumOfValuesInWindow = getRuntimeContext().getMapState(sumDesc);
	}

	@Override
	public void processElement(
			KeyedDataPoint<Double> dataPoint,
			Context ctx,
			Collector<KeyedDataPoint<Double>> out) throws Exception {

		long endOfWindow = setTimer(dataPoint, ctx.timerService());

		Double sum = sumOfValuesInWindow.get(endOfWindow);
		if (sum == null) {
			sum = 0D;
		}
		sum += dataPoint.getValue();
		sumOfValuesInWindow.put(endOfWindow, sum);
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
	public void onTimer(long timestamp, OnTimerContext context, Collector<KeyedDataPoint<Double>> out) throws Exception {
		// Get the timestamp for this timer and use it to look up the sum of the values for that window
		long ts = context.timestamp();
		KeyedDataPoint<Double> result = new KeyedDataPoint<>(context.getCurrentKey(), ts, sumOfValuesInWindow.get(ts));
		out.collect(result);
		sumOfValuesInWindow.remove(timestamp);
	}
}
