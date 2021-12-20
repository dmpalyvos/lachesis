package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;

/**
 * Scheduling policy which assigns threads to cgroups and assigns priority values to each
 * group.
 *
 * @see FairQueryCGroupPolicy
 * @see ClusterinCGroupPolicy
 * @see NoopCGroupPolicy
 * @see OneTasksCGroupPolicy
 * @see MetricQueryCGroupPolicy
 * @see SpeCGroupPolicy
 * @see QueryCGroupPolicy
 * @see OneCGroupPolicy
 * @see OperatorMetricCGroupPolicy
 */
public interface CGroupPolicy {

  void init(Collection<Task> tasks, SpeRuntimeInfo speRuntimeInfo, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider);

  void apply(Collection<Task> tasks,
      SpeRuntimeInfo speRuntimeInfo, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider);

}
