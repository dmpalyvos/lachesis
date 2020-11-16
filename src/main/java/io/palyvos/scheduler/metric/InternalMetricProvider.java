package io.palyvos.scheduler.metric;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * {@link io.palyvos.scheduler.metric.MetricProvider} responsible for computing composite, high-level
 * metrics. It does not fetch data from external systems
 */
class InternalMetricProvider extends AbstractMetricProvider<SchedulerMetric> {

  private static final Logger LOG = LogManager.getLogger(InternalMetricProvider.class);

  /**
   * This collector computes only the non-delegated {@link BaseSchedulerMetric}s
   */
  public static final Map<SchedulerMetric, SchedulerMetric> THREAD_MAPPING = Arrays
      .stream(BaseSchedulerMetric.values())
      .filter(m -> m.isInternal())
      .collect(Collectors.toMap(m -> m, m -> m));
  private final SchedulerMetricProvider schedulerMetricProvider;

  public InternalMetricProvider(SchedulerMetricProvider schedulerMetricProvider) {
    super(THREAD_MAPPING, SchedulerMetric.class);
    Validate.notNull(schedulerMetricProvider, "schedulerMetricProvider");
    this.schedulerMetricProvider = schedulerMetricProvider;
  }

  @Override
  protected void registerDependencies(SchedulerMetric metric) {
    metric.dependencies().forEach(m -> schedulerMetricProvider.register(m));
  }

  @Override
  protected void doCompute(SchedulerMetric metric) {
    metric.compute(schedulerMetricProvider);
  }

}
