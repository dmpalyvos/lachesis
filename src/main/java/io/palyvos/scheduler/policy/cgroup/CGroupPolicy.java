package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;

public interface CGroupPolicy {

  void init(Collection<Task> tasks, SpeRuntimeInfo speRuntimeInfo, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider);

  void apply(Collection<Task> tasks,
      SpeRuntimeInfo speRuntimeInfo, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider);

}
