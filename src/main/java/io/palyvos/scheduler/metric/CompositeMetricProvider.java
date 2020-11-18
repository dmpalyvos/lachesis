package io.palyvos.scheduler.metric;

import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * {@link io.palyvos.scheduler.metric.MetricProvider} responsible for computing composite,
 * high-level metrics. It does not fetch data from external systems
 */
class CompositeMetricProvider extends AbstractMetricProvider<CompositeMetric> {

  private static final Logger LOG = LogManager.getLogger(CompositeMetricProvider.class);

  private final SchedulerMetricProvider schedulerMetricProvider;

  public CompositeMetricProvider(SchedulerMetricProvider schedulerMetricProvider) {
    super(mappingFor(BaseCompositeMetric.values()), CompositeMetric.class);
    Validate.notNull(schedulerMetricProvider, "schedulerMetricProvider");
    this.schedulerMetricProvider = schedulerMetricProvider;
  }

  @Override
  protected void registerDependencies(CompositeMetric metric) {
    metric.compositeDependencies().forEach(schedulerMetricProvider::register);
  }

  @Override
  protected void doCompute(CompositeMetric metric) {
    metric.compositeDependencies().forEach(schedulerMetricProvider::compute);
    metric.compute(schedulerMetricProvider, this);
  }

}
