package io.palyvos.scheduler.metric;

import java.util.Set;

/**
 * A general {@link Metric} that is computed through a composition of other metrics through the use
 * of an internal {@link CompositeMetricProvider} instead of being fetched from the SPE. Known
 * implementations are found at {@link BaseCompositeMetric}.
 *
 * @see BaseCompositeMetric
 * @see CompositeMetricProvider
 */
public interface CompositeMetric extends Metric<CompositeMetric> {

  void compute(SchedulerMetricProvider provider,
      CompositeMetricProvider internalProvider);

  Set<SchedulerMetric> compositeDependencies();
}
