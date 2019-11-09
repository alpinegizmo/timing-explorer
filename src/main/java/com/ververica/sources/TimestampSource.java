package com.ververica.sources;

import com.ververica.data.DataPoint;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class TimestampSource extends RichSourceFunction<DataPoint<Long>> implements CheckpointedFunction {
  private final int periodMs;
  private final int slowdownFactor;
  private volatile boolean running = true;

  // Checkpointed State
  private transient ListState<Long> checkpointedTime;
  private volatile long currentTimeMs = 0;

  public TimestampSource(int periodMs, int slowdownFactor){
    this.periodMs = periodMs;
    this.slowdownFactor = slowdownFactor;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>(
            "checkpointedTime",
            Long.class);

    this.checkpointedTime = context.getOperatorStateStore().getListState(descriptor);

    if (context.isRestored()) {
      for (Long ts : checkpointedTime.get()) {
        this.currentTimeMs = ts;
      }
    } else {
      long now = System.currentTimeMillis();
      this.currentTimeMs = now - (now % 1000); // floor to second boundary
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    checkpointedTime.clear();
    checkpointedTime.add(currentTimeMs);
  }

  @Override
  public void run(SourceContext<DataPoint<Long>> ctx) throws Exception {
    while (running) {
      synchronized (ctx.getCheckpointLock()) {
        ctx.collectWithTimestamp(new DataPoint<>(currentTimeMs, 0L), currentTimeMs);
        currentTimeMs += periodMs;
      }
      timeSync();
    }
  }

  @Override
  public void cancel() {
    running = false;
  }

  private void timeSync() throws InterruptedException {
    // Sync up with real time
    long realTimeDeltaMs = currentTimeMs - System.currentTimeMillis();
    long sleepTime = periodMs + realTimeDeltaMs + randomJitter();

    if (slowdownFactor != 1) {
      sleepTime = periodMs * slowdownFactor;
    }

    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
  }

  private long randomJitter(){
    double sign = -1.0;
    if(Math.random() > 0.5){
      sign = 1.0;
    }
    return (long)(Math.random() * periodMs * sign);
  }
}
