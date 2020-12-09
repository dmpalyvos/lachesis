package io.palyvos.scheduler.policy.translators.cgroup;

import io.palyvos.scheduler.task.CGroup;
import io.palyvos.scheduler.task.CGroupParameterContainer;
import io.palyvos.scheduler.task.ExternalThread;
import java.util.Collection;
import java.util.Map;

public interface CGroupPolicyTranslator {

  void create(Collection<CGroup> cgroups);

  void delete(Collection<CGroup> cgroups);

  void updateParameters(Map<CGroup, Collection<CGroupParameterContainer>> schedule);

  void updateParameter(Map<CGroup, CGroupParameterContainer> schedule);

  void updateAssignment(Map<CGroup, Collection<ExternalThread>> assignment);

}
