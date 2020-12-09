package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;

public interface SinglePrioritySchedulingPolicy {

  void init(SinglePriorityMetricTranslator translator,
      SchedulerMetricProvider metricProvider);

  void apply(Collection<Task> tasks, SinglePriorityMetricTranslator translator,
      SchedulerMetricProvider metricProvider);

}
