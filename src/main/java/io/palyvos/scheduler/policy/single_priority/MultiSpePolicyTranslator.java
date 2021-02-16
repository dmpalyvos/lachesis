package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.task.ExternalThread;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link SinglePriorityMetricTranslator} that does not do any actual translation but instead merges
 * mutliple schedules into one. The {@code run()} function is then called manually to run a delegate
 * translator using the complete schedule.
 */
public class MultiSpePolicyTranslator implements SinglePriorityMetricTranslator {

  private final Map<ExternalThread, Double> mergedSchedule = new HashMap<>();
  private final SinglePriorityMetricTranslator delegate;

  public MultiSpePolicyTranslator(
      SinglePriorityMetricTranslator delegate) {
    this.delegate = delegate;
  }

  public void run() {
    if (mergedSchedule.isEmpty()) {
      return;
    }
    delegate.apply(mergedSchedule);
    mergedSchedule.clear();
  }

  @Override
  public void apply(Map<ExternalThread, Double> schedule) {
    mergedSchedule.putAll(schedule);
  }

}
