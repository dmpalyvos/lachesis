package io.palyvos.scheduler.policy.cgroup;

import static io.palyvos.scheduler.policy.cgroup.CGroupController.CPU;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Query;
import io.palyvos.scheduler.task.QueryResolver;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class QueryCGroupPolicy implements CGroupSchedulingPolicy {

  protected static final CGroup PARENT_CGROUP =
      new CGroup("/" + SchedulerContext.SCHEDULER_NAME, CPU);

  public void apply(Collection<Task> tasks, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {
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

    Map<CGroup, Double> schedule = computeSchedule(metricProvider, queryCgroup);
    translator.apply(schedule);
  }

  protected abstract Map<CGroup, Double> computeSchedule(SchedulerMetricProvider metricProvider,
      Map<Query, CGroup> queryCgroup);
}
