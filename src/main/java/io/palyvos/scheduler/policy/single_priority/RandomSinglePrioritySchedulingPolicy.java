package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Task;
import java.util.concurrent.ThreadLocalRandom;

public class RandomSinglePrioritySchedulingPolicy extends AbstractSinglePrioritySchedulingPolicy {

  public RandomSinglePrioritySchedulingPolicy(boolean scheduleHelpers) {
    super(scheduleHelpers);
  }

  @Override
  public void init(SinglePriorityMetricTranslator translator,
      SchedulerMetricProvider metricProvider) {

  }

  @Override
  protected Double getPriority(SchedulerMetricProvider metricProvider, Task task) {
    return ThreadLocalRandom.current().nextDouble();
  }
}
