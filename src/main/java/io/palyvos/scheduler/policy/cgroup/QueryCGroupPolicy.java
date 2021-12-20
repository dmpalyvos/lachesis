package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Query;
import io.palyvos.scheduler.task.QueryResolver;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Assign each {@link Query} to its own {@link CGroup} with a priority chosen by the
 * subclass.
 */
public abstract class QueryCGroupPolicy implements CGroupPolicy {

  //FIXME: Optimize query resolution
  public void apply(Collection<Task> tasks,
      SpeRuntimeInfo speRuntimeInfo, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {
    final QueryResolver resolver = new QueryResolver(tasks);
    final Map<CGroup, Collection<ExternalThread>> assignment = new HashMap<>();
    final Map<Query, CGroup> queryCgroup = new HashMap<>();
    for (Query query : resolver.queries()) {
      String path = String.valueOf(query.name());
      CGroup cgroup = CGroup.PARENT_CPU_CGROUP.newChild(path);
      Collection<ExternalThread> queryThreads = query.tasks().stream()
          .map(task -> task.threads())
          .flatMap(Collection::stream).collect(Collectors.toList());
      assignment.put(cgroup, queryThreads);
      queryCgroup.put(query, cgroup);
    }

    Map<CGroup, Double> schedule = computeSchedule(metricProvider, queryCgroup);
    translator.assign(assignment);
    translator.apply(schedule);
  }

  protected abstract Map<CGroup, Double> computeSchedule(SchedulerMetricProvider metricProvider,
      Map<Query, CGroup> queryCgroup);
}
