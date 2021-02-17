package io.palyvos.scheduler.policy.cgroup;

import static io.palyvos.scheduler.policy.cgroup.CGroupController.CPU;

import io.palyvos.scheduler.metric.SchedulerMetric;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
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

public class QueryCGroupPolicy implements CGroupSchedulingPolicy {

  public static final String NAME = "QUERY";
  private static final Logger LOG = LogManager.getLogger();

  private static final CGroup PARENT_CGROUP =
      new CGroup("/" + SchedulerContext.SCHEDULER_NAME, CPU);
  private final BiFunction<Query, Map<String, Double>, Double> queryPriorityFunction;
  private final SchedulerMetric metric;

  public QueryCGroupPolicy(
      SchedulerMetric metric,
      BiFunction<Query, Map<String, Double>, Double> queryPriorityFunction) {
    Validate.notNull(queryPriorityFunction, "queryPriorityFunction");
    Validate.notNull(metric, "metric");
    this.metric = metric;
    this.queryPriorityFunction = queryPriorityFunction;
  }



  @Override
  public void init(Collection<Task> tasks, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {
    translator.init(tasks);
    metricProvider.register(metric);
  }

  @Override
  public void apply(Collection<Task> tasks, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {

    final Map<String, Double> taskMetrics = metricProvider.get(metric);

    final QueryResolver resolver = new QueryResolver(tasks);
    final Map<CGroup, Collection<ExternalThread>> assignment = new HashMap<>();
    final Map<Query, CGroup> queryCgroup = new HashMap<>();
    for (Query query : resolver.queries()) {
      String path = String.valueOf(query.name());
      CGroup cgroup = PARENT_CGROUP.newChild(path);
      Collection<ExternalThread> queryThreads = query.tasks().stream()
          .map(task -> task.threads())
          .flatMap(Collection::stream).collect(Collectors.toList());
      assignment.put(cgroup, queryThreads);
      queryCgroup.put(query, cgroup);
    }

    Map<CGroup, Double> schedule = new HashMap<>();
    for (Query query : queryCgroup.keySet()) {
      schedule.put(queryCgroup.get(query), queryPriorityFunction.apply(query, taskMetrics));
    }

    translator.apply(schedule, assignment);
  }
}
