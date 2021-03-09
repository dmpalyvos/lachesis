package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.policy.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CpuQuotaCGroupTranslator implements CGroupTranslator {

  public static final String NAME = "CPU_QUOTA";
  private static final Logger LOG = LogManager.getLogger();
  public static final int MIN_QUOTA = 1;

  private final CGroupActionExecutor cgroupActionExecutor;
  private final CGroupParameterContainer periodParameter;
  private final int ncores;
  private final long period;
  private final DecisionNormalizer normalizer;

  private CGroupScheduleGraphiteReporter graphiteReporter;

  public CpuQuotaCGroupTranslator(
      int ncores, long period,
      DecisionNormalizer normalizer,
      CGroupActionExecutor cgroupActionExecutor) {
    Validate.isTrue(ncores > 0, "ncores <= 0");
    Validate.isTrue(period > 0, "period <= 0");
    Validate.notNull(cgroupActionExecutor, "cgroupActionExecutor");
    Validate.notNull(normalizer, "normalizer");
    this.normalizer = normalizer;
    this.ncores = ncores;
    this.period = period;
    this.cgroupActionExecutor = cgroupActionExecutor;
    this.periodParameter = CGroupParameter.CPU_CFS_PERIOD_US.of(this.period);
  }

  public CpuQuotaCGroupTranslator(int ncores, long period,
      DecisionNormalizer normalizer) {
    this(ncores, period, normalizer, new BasicCGroupActionExecutor());
  }


  @Override
  public void init() {
    CGroup.init(cgroupActionExecutor);
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
    Map<CGroup, Long> normalizedSchedule = normalizer.normalize(schedule);

    double scheduleSum = normalizedSchedule.values().stream().filter(Objects::nonNull)
        .mapToLong(Long::longValue).sum();
    for (CGroup cgroup : normalizedSchedule.keySet()) {
      Long value = normalizedSchedule.get(cgroup);
      if (value == null) {
        LOG.warn("Invalid/missing value for cgroup {}", cgroup.toString());
        continue;
      }
      long quota = Math
          .max(MIN_QUOTA, Math.round((value / scheduleSum) * totalPeriod));
      rawSchedule.put(cgroup,
          Arrays.asList(periodParameter, CGroupParameter.CPU_CFS_QUOTA_US.of(quota)));
    }

    cgroupActionExecutor.updateParameters(rawSchedule);
    graphiteReporter.report(schedule, rawSchedule);
  }

}
