package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.metric.SchedulerMetric;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetricSinglePrioritySchedulingPolicy extends AbstractSinglePrioritySchedulingPolicy {

  private static final Logger LOG = LogManager.getLogger();
  private final SchedulerMetric metric;

  public MetricSinglePrioritySchedulingPolicy(SchedulerMetric metric, boolean scheduleHelpers) {
    super(scheduleHelpers);
    this.metric = metric;
  }

  @Override
  public void init(SinglePriorityMetricTranslator translator,
      SchedulerMetricProvider metricProvider) {
    metricProvider.register(metric);
  }

  @Override
  protected Double getPriority(SchedulerMetricProvider metricProvider, Task task) {
    try {
      return metricProvider.get(metric, task.id());
    } catch (Exception e) {
      LOG.error("Failed to get metric {} for task {}: {}\n", metric, task, e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
