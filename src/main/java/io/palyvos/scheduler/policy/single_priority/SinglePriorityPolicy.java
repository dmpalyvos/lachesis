package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.Map;

/**
 * Scheduling policy which assigns single priority values to threads.
 *
 * @see ConstantSinglePriorityPolicy
 * @see InputOutputQueuePolicy
 * @see MetricSinglePriorityPolicy
 * @see NoopSinglePriorityPolicy
 * @see RandomSinglePriorityPolicy
 */
public interface SinglePriorityPolicy {

  void init(SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider);

  void apply(Collection<Task> tasks,
      SpeRuntimeInfo speRuntimeInfo, SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider);

  Map<ExternalThread, Double> computeSchedule(
      Collection<Task> tasks, SpeRuntimeInfo speRuntimeInfo,
      SchedulerMetricProvider metricProvider);
}
