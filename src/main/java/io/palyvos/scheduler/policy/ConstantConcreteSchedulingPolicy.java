package io.palyvos.scheduler.policy;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.translators.concrete.ConcretePolicyTranslator;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Subtask;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ConstantConcreteSchedulingPolicy implements ConcreteSchedulingPolicy {

  private final long normalizedPriority;

  public ConstantConcreteSchedulingPolicy(long normalizedPriority) {
    this.normalizedPriority = normalizedPriority;
  }

  @Override
  public void apply(Collection<Subtask> subtasks, ConcretePolicyTranslator policyTranslator,
      SchedulerMetricProvider metricProvider) {
    final Map<ExternalThread, Long> normalizedSchedule = new HashMap<>();
    for (Subtask subtask : subtasks) {
      normalizedSchedule.put(subtask.thread(), normalizedPriority);
    }
    policyTranslator.applyDirect(normalizedSchedule);
  }
}
