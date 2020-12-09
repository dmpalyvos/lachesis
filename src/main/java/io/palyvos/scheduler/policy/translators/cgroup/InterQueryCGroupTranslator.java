package io.palyvos.scheduler.policy.translators.cgroup;

import static io.palyvos.scheduler.util.cgroup.CGController.CPU;

import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.metric.graphite.SimpleGraphiteReporter;
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
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InterQueryCGroupTranslator<T extends Metric<T>> {

  private static final Logger LOG = LogManager.getLogger();

  private static final CGroup PARENT_CGROUP = new CGroup(
      "/" + SchedulerContext.SCHEDULER_NAME, CPU);
  private final CGroupPolicyTranslator concreteTranslator;
  private Map<Query, CGroup> cgroupMapping = new HashMap<>();
  private static final String GRAPHITE_PREFIX = "schedule.cgroup";
  private final SimpleGraphiteReporter graphiteReporter = new SimpleGraphiteReporter(
      SchedulerContext.GRAPHITE_STATS_HOST, SchedulerContext.GRAPHITE_STATS_PORT);

  public InterQueryCGroupTranslator(CGroupPolicyTranslator concreteTranslator) {
    this.concreteTranslator = concreteTranslator;
  }

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
    //Logging
    assignment
        .forEach((cgroup, threads) -> LOG.info("{} -> {} threads", cgroup.path(),
            threads.stream().map(t -> t.name()).collect(Collectors.joining(" "))));
    //
    concreteTranslator.create(assignment.keySet());
    concreteTranslator.updateAssignment(assignment);
  }

  public void schedule(Map<String, Double> metricValues,
      BiFunction<Query, Map<String, Double>, Double> queryFunction,
      Function<Map<CGroup, Double>, Map<CGroup, Collection<CGroupParameterContainer>>> scheduleFunction) {
    Map<CGroup, Double> queryMetrics = new HashMap<>();
    for (Query query : cgroupMapping.keySet()) {
      queryMetrics.put(cgroupMapping.get(query), queryFunction.apply(query, metricValues));
    }
    Map<CGroup, Collection<CGroupParameterContainer>> schedule = scheduleFunction.apply(queryMetrics);
    reportToGraphite(queryMetrics, schedule);
    concreteTranslator.updateParameters(schedule);
  }

  private void reportToGraphite(Map<CGroup, Double> queryMetrics,
      Map<CGroup, Collection<CGroupParameterContainer>> schedule) {
    final long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    graphiteReporter.open();
    for (CGroup cgroup : queryMetrics.keySet()) {
      Double queryMetric = queryMetrics.get(cgroup);
      if (queryMetric != null) {
        try {
          graphiteReporter.report(now,
              SimpleGraphiteReporter
                  .schedulerGraphiteKey(GRAPHITE_PREFIX, "internal", cgroup.path()),
              queryMetric);
        } catch (Exception e) {
          LOG.warn("Failed to report to graphite: {}", e.getMessage());
        }
      }
      Collection<CGroupParameterContainer> parameters = schedule.get(cgroup);
      if (parameters != null) {
        for (CGroupParameterContainer parameter : parameters) {
          try {
            graphiteReporter.report(now,
                SimpleGraphiteReporter
                    .schedulerGraphiteKey(GRAPHITE_PREFIX, "external",
                        parameter.key().replace(".", "_") + "." +
                            cgroup.path()),
                parameter.value());
          } catch (Exception e) {
            LOG.warn("Failed to report to graphite: {}", e.getMessage());
          }
        }
      }
    }
    graphiteReporter.close();
  }

}
