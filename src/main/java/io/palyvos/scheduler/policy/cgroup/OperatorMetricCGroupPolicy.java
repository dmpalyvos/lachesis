package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
import io.palyvos.scheduler.metric.SchedulerMetric;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link CGroupPolicy} that assigns each {@link Task} to its own {@link CGroup} with the priority
 * given by a user-chosen {@link SchedulerMetric}.
 */
public class OperatorMetricCGroupPolicy implements CGroupPolicy {

  public static final String NAME = "OPERATOR-METRIC";
  private final SchedulerMetric metric;
  private Map<String, CGroup> taskCgroup = new HashMap<>();

  public OperatorMetricCGroupPolicy(SchedulerMetric metric) {
    this.metric = metric;
  }

  @Override
  public void init(Collection<Task> tasks, SpeRuntimeInfo speRuntimeInfo,
      CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {
    translator.init();
    translator.assign(oneCGroupPerTask(tasks));
    metricProvider.register(metric);
  }

  private Map<CGroup, Collection<ExternalThread>> oneCGroupPerTask(Collection<Task> tasks) {
    final Map<CGroup, Collection<ExternalThread>> assignment = new HashMap<>();
    for (Task task : tasks) {
      CGroup cgroup = CGroup.PARENT_CPU_CGROUP.newChild(task.id());
      assignment.put(cgroup, task.threads());
      taskCgroup.put(task.id(), cgroup);
    }
    return assignment;
  }

  @Override
  public void apply(Collection<Task> tasks,
      SpeRuntimeInfo speRuntimeInfo, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {
    final Map<String, Double> taskMetrics = metricProvider.get(metric);
    Map<CGroup, Double> schedule = new HashMap<>();
    for (Task task : tasks) {
      schedule.put(taskCgroup.get(task.id()), taskMetrics.get(task.id()));
    }
    translator.apply(schedule);
  }
}
