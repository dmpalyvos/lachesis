package io.palyvos.scheduler.policy.cgroup;

import static io.palyvos.scheduler.util.cgroup.CGController.CPU;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Query;
import io.palyvos.scheduler.task.QueryResolver;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryCGroupMetricTranslator implements CGroupMetricTranslator {

  public static final String NAME = "QUERY";
  private static final Logger LOG = LogManager.getLogger();

  private static final CGroup PARENT_CGROUP =
      new CGroup("/" + SchedulerContext.SCHEDULER_NAME, CPU);
  private final CGroupActionExecutor cgroupActionExecutor;
  private final BiFunction<Query, Map<String, Double>, Double> queryPriorityFunction;
  private Map<Query, CGroup> cgroupMapping = new HashMap<>();
  private final CGroupScheduleGraphiteReporter graphiteReporter = new CGroupScheduleGraphiteReporter(
      SchedulerContext.GRAPHITE_STATS_HOST, SchedulerContext.GRAPHITE_STATS_PORT);

  public QueryCGroupMetricTranslator(BiFunction<Query, Map<String, Double>, Double> queryPriorityFunction,
      CGroupActionExecutor cgroupActionExecutor) {
    Validate.notNull(queryPriorityFunction, "queryPriorityFunction");
    Validate.notNull(cgroupActionExecutor, "cgroupActionExecutor");
    this.cgroupActionExecutor = cgroupActionExecutor;
    this.queryPriorityFunction = queryPriorityFunction;
  }

  public QueryCGroupMetricTranslator(BiFunction<Query, Map<String, Double>, Double> queryPriorityFunction) {
    this(queryPriorityFunction, new BasicCGroupActionExecutor());
  }

  @Override
  public void init(Collection<Task> tasks) {
    QueryResolver resolver = new QueryResolver(tasks);
    Map<CGroup, Collection<ExternalThread>> assignment = new HashMap<>();
    cgroupMapping.clear();
    for (Query query : resolver.queries()) {
      String path = String.valueOf(query.name());
      CGroup cgroup = PARENT_CGROUP.newChild(path);
      Collection<ExternalThread> queryThreads = query.tasks().stream()
          .map(task -> task.threads())
          .flatMap(Collection::stream).collect(Collectors.toList());
      assignment.put(cgroup, queryThreads);
      cgroupMapping.put(query, cgroup);
    }
    cgroupActionExecutor.create(assignment.keySet());
    cgroupActionExecutor.updateAssignment(assignment);
  }

  @Override
  public void apply(Map<String, Double> metricValues, CGroupPriorityToParametersFunction priorityToParametersFunction) {
    Map<CGroup, Double> queryMetrics = new HashMap<>();
    for (Query query : cgroupMapping.keySet()) {
      queryMetrics.put(cgroupMapping.get(query), queryPriorityFunction.apply(query, metricValues));
    }
    Map<CGroup, Collection<CGroupParameterContainer>> schedule = priorityToParametersFunction
        .apply(queryMetrics);
    graphiteReporter.report(queryMetrics, schedule);
    cgroupActionExecutor.updateParameters(schedule);
  }


}
