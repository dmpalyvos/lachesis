package io.palyvos.scheduler.policy.translators.cgroup;

import io.palyvos.scheduler.task.CGroupParameter;
import io.palyvos.scheduler.task.CGroup;
import io.palyvos.scheduler.task.CGroupParameterContainer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CpuSharesScheduleFunction implements
    Function<Map<CGroup, Double>, Map<CGroup, Collection<CGroupParameterContainer>>> {

  private static final Logger LOG = LogManager.getLogger();
  private final Function<Double, Double> preprocessFunction;

  public CpuSharesScheduleFunction() {
    this.preprocessFunction = value -> value;
  }

  public CpuSharesScheduleFunction(Function<Double, Double> preprocessFunction) {
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
