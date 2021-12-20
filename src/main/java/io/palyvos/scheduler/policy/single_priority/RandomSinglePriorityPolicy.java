package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Task;
import java.util.concurrent.ThreadLocalRandom;

/**
 * {@link SinglePriorityPolicy} that assigns random priorities to {@link Task}s.
 */
public class RandomSinglePriorityPolicy extends AbstractSinglePriorityPolicy {

  public RandomSinglePriorityPolicy(boolean scheduleHelpers) {
    super(scheduleHelpers);
  }

  @Override
  public void init(SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider) {

  }

  @Override
  protected Double getPriority(SchedulerMetricProvider metricProvider, Task task) {
    return ThreadLocalRandom.current().nextDouble();
  }
}
