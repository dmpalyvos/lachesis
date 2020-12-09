package io.palyvos.scheduler.policy.translators.cgroup;

import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.Map;

public interface CGroupAgnosticTranslator {

  void init(Collection<Task> tasks);

  void schedule(Map<String, Double> metricValues, CGroupSchedulingFunction scheduleFunction);
}
