package io.palyvos.scheduler.metric;

public interface SchedulerMetric extends Metric<SchedulerMetric> {

  boolean isInternal();

  void compute(SchedulerMetricProvider provider);
}
