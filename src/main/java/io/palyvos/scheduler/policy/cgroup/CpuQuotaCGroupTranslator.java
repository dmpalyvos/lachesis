package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CpuQuotaCGroupTranslator implements CGroupTranslator {

  public static final String NAME = "CPU_QUOTA";
  private static final Logger LOG = LogManager.getLogger();

  private final CGroupActionExecutor cgroupActionExecutor;
  private final CGroupParameterContainer periodParameter;
  private final int ncores;
  private final long period;
  private final Function<Double, Double> preprocessFunction;

  private CGroupScheduleGraphiteReporter graphiteReporter;

  public CpuQuotaCGroupTranslator(
      int ncores, long period,
      Function<Double, Double> preprocessFunction,
      CGroupActionExecutor cgroupActionExecutor) {
    Validate.isTrue(ncores > 0, "ncores <= 0");
    Validate.isTrue(period > 0, "period <= 0");
    Validate.notNull(preprocessFunction, "preprocessFunction");
    Validate.notNull(cgroupActionExecutor, "cgroupActionExecutor");
    this.ncores = ncores;
    this.period = period;
    this.preprocessFunction = preprocessFunction;
    this.cgroupActionExecutor = cgroupActionExecutor;
    this.periodParameter = CGroupParameter.CPU_CFS_PERIOD_US.of(this.period);
  }

  public CpuQuotaCGroupTranslator(
      int ncores, long period,
      Function<Double, Double> preprocessFunction) {
    this(ncores, period, preprocessFunction, new BasicCGroupActionExecutor());
  }

  public CpuQuotaCGroupTranslator(
      int ncores, long period) {
    this(ncores, period, s -> s, new BasicCGroupActionExecutor());
  }


  @Override
  public void init(Collection<Task> tasks) {
    graphiteReporter = new CGroupScheduleGraphiteReporter(
        SchedulerContext.GRAPHITE_STATS_HOST, SchedulerContext.GRAPHITE_STATS_PORT);
  }

  @Override
  public void assign(Map<CGroup, Collection<ExternalThread>> assignment) {
    cgroupActionExecutor.create(assignment.keySet());
    cgroupActionExecutor.updateAssignment(assignment);
  }


  @Override
  public void apply(Map<CGroup, Double> schedule) {

    final long totalPeriod = period * ncores;
    Map<CGroup, Collection<CGroupParameterContainer>> rawSchedule = new HashMap<>();

    double scheduleSum = schedule.values().stream().filter(Objects::nonNull)
        .map(preprocessFunction)
        .mapToDouble(Double::doubleValue).sum();
    for (CGroup cgroup : schedule.keySet()) {
      Double value = schedule.get(cgroup);
      if (value == null || !Double.isFinite(value)) {
        LOG.warn("Invalid/missing value for cgroup {}", cgroup.toString());
        continue;
      }
      final double preprocessedValue = preprocessFunction.apply(value);
      long normalizedValue = Math
          .max(1, Math.round((preprocessedValue / scheduleSum) * totalPeriod));
      rawSchedule.put(cgroup,
          Arrays.asList(periodParameter, CGroupParameter.CPU_CFS_QUOTA_US.of(normalizedValue)));
    }

    cgroupActionExecutor.updateParameters(rawSchedule);
    graphiteReporter.report(schedule, rawSchedule);
  }

}
