package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CpuSharesCGroupTranslator implements CGroupTranslator {

  public static final String NAME = "CPU_SHARES";
  private static final Logger LOG = LogManager.getLogger();

  private final CGroupActionExecutor cgroupActionExecutor;
  private final Function<Double, Double> preprocessFunction;
  private CGroupScheduleGraphiteReporter graphiteReporter;


  public CpuSharesCGroupTranslator(
      Function<Double, Double> preprocessFunction,
      CGroupActionExecutor cgroupActionExecutor) {
    Validate.notNull(preprocessFunction, "preprocessFunction");
    Validate.notNull(cgroupActionExecutor, "cgroupActionExecutor");
    this.preprocessFunction = preprocessFunction;
    this.cgroupActionExecutor = cgroupActionExecutor;
  }

  public CpuSharesCGroupTranslator(Function<Double, Double> preprocessFunction) {
    this(preprocessFunction, new BasicCGroupActionExecutor());
  }

  public CpuSharesCGroupTranslator() {
    this(s -> s, new BasicCGroupActionExecutor());
  }

  @Override
  public void init(Collection<Task> tasks) {
    graphiteReporter = new CGroupScheduleGraphiteReporter(
        SchedulerContext.GRAPHITE_STATS_HOST, SchedulerContext.GRAPHITE_STATS_PORT);
  }


  @Override
  public void apply(Map<CGroup, Double> schedule,
      Map<CGroup, Collection<ExternalThread>> assignment) {
    cgroupActionExecutor.create(schedule.keySet());
    cgroupActionExecutor.updateAssignment(assignment);

    Map<CGroup, Collection<CGroupParameterContainer>> rawSchedule = new HashMap<>();
    for (CGroup cgroup : schedule.keySet()) {
      Double value = schedule.get(cgroup);
      if (value == null || !Double.isFinite(value)) {
        LOG.warn("Invalid/missing value for cgroup {}", cgroup.toString());
        continue;
      }
      final Double preprocessedValue = preprocessFunction.apply(value);
      long convertedValue = Math.max(2, Math.round(preprocessedValue));
      rawSchedule.put(cgroup, Arrays.asList(CGroupParameter.CPU_SHARES.of(convertedValue)));
    }

    cgroupActionExecutor.updateParameters(rawSchedule);
    graphiteReporter.report(schedule, rawSchedule);
  }

}
