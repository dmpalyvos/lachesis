package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.policy.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CpuSharesCGroupTranslator implements CGroupTranslator {

  public static final String NAME = "CPU-SHARES";
  private static final Logger LOG = LogManager.getLogger();
  public static final int MIN_SHARES = 2;

  private final CGroupActionExecutor cgroupActionExecutor;
  private final DecisionNormalizer normalizer;
  private CGroupScheduleGraphiteReporter graphiteReporter;


  public CpuSharesCGroupTranslator(
      DecisionNormalizer normalizer,
      CGroupActionExecutor cgroupActionExecutor) {
    Validate.notNull(normalizer, "normalizer");
    Validate.notNull(cgroupActionExecutor, "cgroupActionExecutor");
    this.normalizer = normalizer;
    this.cgroupActionExecutor = cgroupActionExecutor;
  }

  public CpuSharesCGroupTranslator(
      DecisionNormalizer normalizer) {
    this(normalizer, new BasicCGroupActionExecutor());
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

    Map<CGroup, Long> normalizedSchedule = normalizer.normalize(schedule);
    Map<CGroup, Collection<CGroupParameterContainer>> rawSchedule = new HashMap<>();
    for (CGroup cgroup : normalizedSchedule.keySet()) {
      Long value = normalizedSchedule.get(cgroup);
      if (value == null) {
        LOG.warn("Invalid/missing value for cgroup {}", cgroup.toString());
        continue;
      }
      long shares = Math.max(MIN_SHARES, value);
      rawSchedule.put(cgroup, Arrays.asList(CGroupParameter.CPU_SHARES.of(shares)));
    }
    cgroupActionExecutor.updateParameters(rawSchedule);
    graphiteReporter.report(schedule, rawSchedule);
  }

}
