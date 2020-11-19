package io.palyvos.scheduler.policy;

import io.palyvos.scheduler.metric.SchedulerMetric;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.translators.concrete.ConcretePolicyTranslator;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Subtask;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetricConcreteSchedulingPolicy implements ConcreteSchedulingPolicy {

  private static final Logger LOG = LogManager.getLogger();
  private final SchedulerMetric metric;

  public MetricConcreteSchedulingPolicy(SchedulerMetric metric) {
    this.metric = metric;
  }

  @Override
  public void apply(Collection<Subtask> subtasks, ConcretePolicyTranslator policyTranslator,
      SchedulerMetricProvider metricProvider) {
    final Map<ExternalThread, Double> schedule = new HashMap<>();
    for (Subtask subtask : subtasks) {
      try {
        final double priority = metricProvider.get(metric, subtask.id());
        schedule.put(subtask.thread(), priority);
      } catch (Exception e) {
        LOG.error("Failed to get metric {} for task {}: {}\n", metric, subtask, e.getMessage());
        throw new RuntimeException(e);
      }
    }
    policyTranslator.applyPolicy(schedule);
  }
}
