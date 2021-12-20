package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
import io.palyvos.scheduler.metric.SchedulerMetric;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Query;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * {@link QueryCGroupPolicy} where each query is assigned to its own {@link CGroup} and the priority
 * computed through a user defined function of the query and the metric values.
 */
public class MetricQueryCGroupPolicy extends QueryCGroupPolicy {

  public static final String NAME = "QUERY-METRIC";
  private static final Logger LOG = LogManager.getLogger();

  private final BiFunction<Query, Map<String, Double>, Double> queryPriorityFunction;
  private final SchedulerMetric metric;

  /**
   * Construct.
   *
   * @param metric                The metric to be used for priority computations.
   * @param queryPriorityFunction {@link BiFunction} that takes as inputs the {@link Query} and the
   *                              metric values and returns the priority for the query.
   */
  public MetricQueryCGroupPolicy(
      SchedulerMetric metric,
      BiFunction<Query, Map<String, Double>, Double> queryPriorityFunction) {
    Validate.notNull(queryPriorityFunction, "queryPriorityFunction");
    Validate.notNull(metric, "metric");
    this.metric = metric;
    this.queryPriorityFunction = queryPriorityFunction;
  }


  @Override
  public void init(Collection<Task> tasks, SpeRuntimeInfo speRuntimeInfo,
      CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {
    translator.init();
    metricProvider.register(metric);
  }

  @Override
  protected Map<CGroup, Double> computeSchedule(SchedulerMetricProvider metricProvider,
      Map<Query, CGroup> queryCgroup) {
    final Map<String, Double> taskMetrics = metricProvider.get(metric);
    Map<CGroup, Double> schedule = new HashMap<>();
    for (Query query : queryCgroup.keySet()) {
      schedule.put(queryCgroup.get(query), queryPriorityFunction.apply(query, taskMetrics));
    }
    return schedule;
  }
}
