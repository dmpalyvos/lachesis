package io.palyvos.scheduler.policy.translators.cgroup;

import io.palyvos.scheduler.task.CGroup;
import io.palyvos.scheduler.task.CGroupParameterContainer;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

public interface CGroupSchedulingFunction extends
    Function<Map<CGroup, Double>, Map<CGroup, Collection<CGroupParameterContainer>>> {

}
