package io.palyvos.scheduler.metric;

public enum MockMetric implements Metric<MockMetric> {
  SUBTASK_TUPLES_IN_RECENT,
  SUBTASK_TUPLES_OUT_RECENT,
  SUBTASK_TUPLES_IN_TOTAL,
  SUBTASK_TUPLES_OUT_TOTAL,
  THREAD_CPU_UTILIZATION;
}
