package io.palyvos.scheduler.policy;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.translators.concrete.ConcretePolicyTranslator;
import io.palyvos.scheduler.task.Subtask;
import java.util.Collection;

public interface ConcreteSchedulingPolicy {

  void apply(Collection<Subtask> subtasks, ConcretePolicyTranslator policyTranslator,
      SchedulerMetricProvider metricProvider);

}
