package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.Map;

public class NoopSinglePrioritySchedulingPolicy implements
    SinglePrioritySchedulingPolicy {

  @Override
  public void init(SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider) {

  }

  @Override
  public void apply(Collection<Task> tasks, SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider) {

  }

  @Override
  public Map<ExternalThread, Double> computeSchedule(Collection<Task> tasks,
      SchedulerMetricProvider metricProvider) {
    return null;
  }
}
