package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;

public class NoopCGroupSchedulingPolicy implements
    CGroupSchedulingPolicy {

  @Override
  public void init(Collection<Task> tasks,
      CGroupTranslator translator, SchedulerMetricProvider metricProvider) {

  }

  @Override
  public void apply(Collection<Task> tasks, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {

  }
}
