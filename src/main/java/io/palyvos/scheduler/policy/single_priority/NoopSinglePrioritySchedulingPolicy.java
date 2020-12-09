package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;

public class NoopSinglePrioritySchedulingPolicy implements
    SinglePrioritySchedulingPolicy {

  @Override
  public void init(SinglePriorityMetricTranslator translator,
      SchedulerMetricProvider metricProvider) {

  }

  @Override
  public void apply(Collection<Task> tasks, SinglePriorityMetricTranslator translator,
      SchedulerMetricProvider metricProvider) {

  }
}
