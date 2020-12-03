package io.palyvos.scheduler.policy;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.translators.concrete.ConcretePolicyTranslator;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;

public interface ConcreteSchedulingPolicy {

  void init(ConcretePolicyTranslator policyTranslator,
      SchedulerMetricProvider metricProvider);

  void apply(Collection<Task> tasks, ConcretePolicyTranslator policyTranslator,
      SchedulerMetricProvider metricProvider);

}
