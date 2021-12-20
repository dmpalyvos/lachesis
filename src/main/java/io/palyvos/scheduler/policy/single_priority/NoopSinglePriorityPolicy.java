package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.Map;

/**
 * {@link SinglePriorityPolicy} that does nothing.
 */
public class NoopSinglePriorityPolicy implements
    SinglePriorityPolicy {

  @Override
  public void init(SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider) {

  }

  @Override
  public void apply(Collection<Task> tasks,
      SpeRuntimeInfo speRuntimeInfo, SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider) {

  }

  @Override
  public Map<ExternalThread, Double> computeSchedule(
      Collection<Task> tasks, SpeRuntimeInfo speRuntimeInfo,
      SchedulerMetricProvider metricProvider) {
    return null;
  }
}
