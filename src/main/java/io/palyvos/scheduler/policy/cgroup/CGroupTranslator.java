package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.Map;

public interface CGroupTranslator {

  void init(Collection<Task> tasks);

  void apply(Map<CGroup, Double> schedule, Map<CGroup, Collection<ExternalThread>> assignment);
}
