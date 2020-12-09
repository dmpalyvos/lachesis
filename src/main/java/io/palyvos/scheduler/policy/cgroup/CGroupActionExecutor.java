package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.task.ExternalThread;
import java.util.Collection;
import java.util.Map;

public interface CGroupActionExecutor {

  void create(Collection<CGroup> cgroups);

  void delete(Collection<CGroup> cgroups);

  void updateParameters(Map<CGroup, Collection<CGroupParameterContainer>> schedule);

  void updateAssignment(Map<CGroup, Collection<ExternalThread>> assignment);

}
