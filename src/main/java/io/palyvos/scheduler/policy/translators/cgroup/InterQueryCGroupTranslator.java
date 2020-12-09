package io.palyvos.scheduler.policy.translators.cgroup;

import static io.palyvos.scheduler.util.cgroup.CGController.CPU;

import io.palyvos.scheduler.task.CGroup;
import io.palyvos.scheduler.task.CGroupParameterContainer;
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

public class InterQueryCGroupTranslator implements CGroupAgnosticTranslator {

  public static final String NAME = "QUERY";
  private static final Logger LOG = LogManager.getLogger();

  private static final CGroup PARENT_CGROUP =
      new CGroup("/" + SchedulerContext.SCHEDULER_NAME, CPU);
  private final CGroupPolicyTranslator policyTranslator;
  private final BiFunction<Query, Map<String, Double>, Double> queryFunction;
  private Map<Query, CGroup> cgroupMapping = new HashMap<>();
  private final CGroupScheduleGraphiteReporter graphiteReporter = new CGroupScheduleGraphiteReporter(
      SchedulerContext.GRAPHITE_STATS_HOST, SchedulerContext.GRAPHITE_STATS_PORT);

  public InterQueryCGroupTranslator(BiFunction<Query, Map<String, Double>, Double> queryFunction,
      CGroupPolicyTranslator policyTranslator) {
    Validate.notNull(queryFunction, "queryFunction");
    Validate.notNull(policyTranslator, "policyTranslator");
    this.policyTranslator = policyTranslator;
    this.queryFunction = queryFunction;
  }

  public InterQueryCGroupTranslator(BiFunction<Query, Map<String, Double>, Double> queryFunction) {
    this(queryFunction, new BasicCGroupPolicyTranslator());
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
    policyTranslator.create(assignment.keySet());
    policyTranslator.updateAssignment(assignment);
  }

  @Override
  public void schedule(Map<String, Double> metricValues, CGroupSchedulingFunction scheduleFunction) {
    Map<CGroup, Double> queryMetrics = new HashMap<>();
    for (Query query : cgroupMapping.keySet()) {
      queryMetrics.put(cgroupMapping.get(query), queryFunction.apply(query, metricValues));
    }
    Map<CGroup, Collection<CGroupParameterContainer>> schedule = scheduleFunction
        .apply(queryMetrics);
    graphiteReporter.report(queryMetrics, schedule);
    policyTranslator.updateParameters(schedule);
  }


}
