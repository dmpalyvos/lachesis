package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.Map;

public interface SinglePriorityPolicy {

  void init(SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider);

  void apply(Collection<Task> tasks, SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider);

  Map<ExternalThread, Double> computeSchedule(Collection<Task> tasks,
      SchedulerMetricProvider metricProvider);
}
