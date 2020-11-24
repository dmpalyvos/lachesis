package io.palyvos.scheduler.policy;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.translators.concrete.ConcretePolicyTranslator;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Subtask;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class RandomConcreteSchedulingPolicy implements ConcreteSchedulingPolicy {

  @Override
  public void init(ConcretePolicyTranslator policyTranslator,
      SchedulerMetricProvider metricProvider) {
  }

  @Override
  public void apply(Collection<Subtask> subtasks, ConcretePolicyTranslator policyTranslator,
      SchedulerMetricProvider metricProvider) {
    final Map<ExternalThread, Double> schedule = new HashMap<>();
    for (Subtask subtask : subtasks) {
      final double priority = ThreadLocalRandom.current().nextDouble();
      schedule.put(subtask.thread(), priority);
    }
    policyTranslator.applyPolicy(schedule);
  }
}
