package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;

public interface MultiSpeSinglePrioritySchedulingPolicy {

  void init(SinglePriorityTranslator translator,
      Collection<SchedulerMetricProvider> metricProviders);

  void update(Collection<Task> tasks, SchedulerMetricProvider metricProvider, double scalingFactor);

  void apply(SinglePriorityTranslator translator);

  void reset();

}