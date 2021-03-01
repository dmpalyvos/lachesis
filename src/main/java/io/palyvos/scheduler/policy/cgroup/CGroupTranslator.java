package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.Map;

public interface CGroupTranslator {

  void init(Collection<Task> tasks);

  void assign(Map<CGroup, Collection<ExternalThread>> assignment);

  void apply(Map<CGroup, Double> schedule);
}
