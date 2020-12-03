package io.palyvos.scheduler.policy;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.translators.concrete.ConcretePolicyTranslator;
import io.palyvos.scheduler.task.Task;
import java.util.concurrent.ThreadLocalRandom;

public class RandomConcreteSchedulingPolicy extends AbstractConcreteSchedulingPolicy {

  public RandomConcreteSchedulingPolicy(boolean scheduleHelpers) {
    super(scheduleHelpers);
  }

  @Override
  public void init(ConcretePolicyTranslator policyTranslator,
      SchedulerMetricProvider metricProvider) {

  }

  @Override
  protected Double getPriority(SchedulerMetricProvider metricProvider, Task task) {
    return ThreadLocalRandom.current().nextDouble();
  }
}
