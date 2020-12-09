package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;

public interface CGroupSchedulingPolicy {

  void init(Collection<Task> tasks,
      SchedulerMetricProvider metricProvider);

  void apply(Collection<Task> tasks, SchedulerMetricProvider metricProvider);

}
