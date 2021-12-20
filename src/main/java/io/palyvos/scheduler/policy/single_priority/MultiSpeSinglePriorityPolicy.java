package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;

@Deprecated
public interface MultiSpeSinglePriorityPolicy {

  void init(SinglePriorityTranslator translator,
      Collection<SchedulerMetricProvider> metricProviders);

  void update(Collection<Task> tasks, SpeRuntimeInfo speRuntimeInfo,
      SchedulerMetricProvider metricProvider,
      double scalingFactor);

  void apply(SinglePriorityTranslator translator);

  void reset();

}