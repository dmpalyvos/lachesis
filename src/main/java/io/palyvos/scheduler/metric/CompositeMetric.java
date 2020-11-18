package io.palyvos.scheduler.metric;

import java.util.Set;

public interface CompositeMetric extends Metric<CompositeMetric> {

  void compute(SchedulerMetricProvider provider,
      CompositeMetricProvider internalProvider);

  Set<SchedulerMetric> compositeDependencies();
}
