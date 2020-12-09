package io.palyvos.scheduler.policy.cgroup;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CGroupPriorityToCpuShares implements
    CGroupPriorityToParametersFunction {

  public static final String NAME = "CPU_SHARES";
  private static final Logger LOG = LogManager.getLogger();
  private final Function<Double, Double> preprocessFunction;

  public CGroupPriorityToCpuShares() {
    this.preprocessFunction = value -> value;
  }

  public CGroupPriorityToCpuShares(Function<Double, Double> preprocessFunction) {
    this.preprocessFunction = preprocessFunction;
  }

  @Override
  public Map<CGroup, Collection<CGroupParameterContainer>> apply(Map<CGroup, Double> cgroupValues) {
    Map<CGroup, Collection<CGroupParameterContainer>> schedule = new HashMap<>();
    for (CGroup cgroup : cgroupValues.keySet()) {
      Double value = cgroupValues.get(cgroup);
      if (value == null || !Double.isFinite(value)) {
        LOG.warn("Invalid/missing value for cgroup {}", cgroup.toString());
        continue;
      }
      final Double preprocessedValue = preprocessFunction.apply(value);
      long convertedValue = Math.max(2, Math.round(preprocessedValue));
      schedule.put(cgroup, Arrays.asList(CGroupParameter.CPU_SHARES.of(convertedValue)));
    }
    return schedule;
  }
}
