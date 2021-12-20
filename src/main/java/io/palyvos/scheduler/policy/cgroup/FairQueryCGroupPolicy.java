package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Query;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link QueryCGroupPolicy} that assigns each {@link Query} to its own {@link CGroup} and gives
 * each of these the same priority.
 */
public class FairQueryCGroupPolicy extends
    QueryCGroupPolicy {

  public static final String NAME = "QUERY-FAIR";
  private static final double PRIORITY = 1024;

  @Override
  public void init(Collection<Task> tasks, SpeRuntimeInfo speRuntimeInfo,
      CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {
    translator.init();
  }

  @Override
  protected Map<CGroup, Double> computeSchedule(SchedulerMetricProvider metricProvider,
      Map<Query, CGroup> queryCgroup) {
    Map<CGroup, Double> schedule = new HashMap<>();
    for (Query query : queryCgroup.keySet()) {
      schedule.put(queryCgroup.get(query), PRIORITY);
    }
    return schedule;
  }
}
