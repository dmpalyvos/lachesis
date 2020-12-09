package io.palyvos.scheduler.policy.cgroup;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

/**
 * Function that transforms numeric cgroup priorities to low-level {@link CGroupParameter}s.
 */
public interface CGroupPriorityToParametersFunction extends
    Function<Map<CGroup, Double>, Map<CGroup, Collection<CGroupParameterContainer>>> {

}
