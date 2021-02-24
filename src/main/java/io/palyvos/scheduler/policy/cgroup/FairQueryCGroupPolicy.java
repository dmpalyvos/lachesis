package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Query;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class FairQueryCGroupPolicy extends
    QueryCGroupPolicy {

  public static final String NAME = "FAIR";
  private static final double PRIORITY = 100;

  @Override
  public void init(Collection<Task> tasks, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {
    translator.init(tasks);
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
