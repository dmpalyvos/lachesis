package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.Map;

public interface CGroupMetricTranslator {

  void init(Collection<Task> tasks);

  void apply(Map<String, Double> metricValues,
      CGroupPriorityToParametersFunction priorityToParametersFunction);
}
