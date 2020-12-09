package io.palyvos.scheduler.policy;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;

public class CGroupNoopPolicy implements
    CGroupSchedulingPolicy {

  @Override
  public void init(Collection<Task> tasks,
      SchedulerMetricProvider metricProvider) {

  }

  @Override
  public void apply(Collection<Task> tasks, SchedulerMetricProvider metricProvider) {

  }
}
