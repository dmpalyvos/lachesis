package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;

public interface SinglePrioritySchedulingPolicy {

  void init(SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider);

  void apply(Collection<Task> tasks, SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider);

}
