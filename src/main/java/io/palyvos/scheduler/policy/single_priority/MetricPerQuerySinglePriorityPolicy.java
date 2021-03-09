package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
import io.palyvos.scheduler.metric.SchedulerMetric;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Query;
import io.palyvos.scheduler.task.QueryResolver;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The same as {@link io.palyvos.scheduler.policy.single_priority.MetricSinglePriorityPolicy} but
 * priorities are normalized on a per-query basis (useful for mixing with {@link
 * io.palyvos.scheduler.policy.cgroup.CGroupPolicy}
 */
public class MetricPerQuerySinglePriorityPolicy extends
    MetricSinglePriorityPolicy {

  private static final Logger LOG = LogManager.getLogger();

  private QueryResolver resolver;

  public MetricPerQuerySinglePriorityPolicy(SchedulerMetric metric, boolean scheduleHelpers) {
    super(metric, scheduleHelpers);
  }

  @Override
  public void apply(Collection<Task> tasks,
      SpeRuntimeInfo speRuntimeInfo, SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider) {
    if (resolver == null) {
      resolver = new QueryResolver(tasks);
    }
    for (Query query : resolver.queries()) {
      super.apply(query.tasks(), speRuntimeInfo, translator, metricProvider);
    }

  }

}
