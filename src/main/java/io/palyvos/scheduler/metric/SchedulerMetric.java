package io.palyvos.scheduler.metric;

/**
 * Public-facing metric that can be registered with the top-level {@link MetricProvider}, {@link
 * SchedulerMetricProvider}. Known metrics are implementations of this interface defined at {@link
 * BasicSchedulerMetric}.
 *
 * @see BasicSchedulerMetric
 */
public interface SchedulerMetric extends Metric<SchedulerMetric> {

}
