package io.palyvos.scheduler.policy.cgroup;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CGroupPriorityToCpuQuota implements
    CGroupPriorityToParametersFunction {

  public static final String NAME = "CPU_QUOTA";
  private static final Logger LOG = LogManager.getLogger();
  private final CGroupParameterContainer periodParameter;
  private final int ncores;
  private final long period;
  private final Function<Double, Double> preprocessFunction;

  public CGroupPriorityToCpuQuota(long period, int ncores) {
    this.period = period;
    this.periodParameter = CGroupParameter.CPU_CFS_PERIOD_US.of(period);
    this.ncores = ncores;
    this.preprocessFunction = value -> value;
  }

  public CGroupPriorityToCpuQuota(long period, int ncores, Function<Double, Double> preprocessFunction) {
    this.period = period;
    this.periodParameter = CGroupParameter.CPU_CFS_PERIOD_US.of(period);
    this.ncores = ncores;
    this.preprocessFunction = preprocessFunction;
  }

  @Override
  public Map<CGroup, Collection<CGroupParameterContainer>> apply(Map<CGroup, Double> cgroupValues) {
    Map<CGroup, Collection<CGroupParameterContainer>> schedule = new HashMap<>();

    final long totalPeriod = period * ncores;

    double cgroupValuesSum = cgroupValues.values().stream().filter(Objects::nonNull)
        .map(preprocessFunction)
        .mapToDouble(Double::doubleValue).sum();
    for (CGroup cgroup : cgroupValues.keySet()) {
      Double value = cgroupValues.get(cgroup);
      if (value == null || !Double.isFinite(value)) {
        LOG.warn("Invalid/missing value for cgroup {}", cgroup.toString());
        continue;
      }
      final double preprocessedValue = preprocessFunction.apply(value);
      long normalizedValue = Math
          .max(1, Math.round((preprocessedValue / cgroupValuesSum) * totalPeriod));
      schedule.put(cgroup,
          Arrays.asList(periodParameter, CGroupParameter.CPU_CFS_QUOTA_US.of(normalizedValue)));
    }
    return schedule;
  }
}
