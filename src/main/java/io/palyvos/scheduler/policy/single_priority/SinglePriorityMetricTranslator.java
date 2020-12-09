package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.task.ExternalThread;
import java.util.Map;

public interface SinglePriorityMetricTranslator {

  void applyPolicy(Map<ExternalThread, Double> schedule);

  int applyDirect(Map<ExternalThread, Long> normalizedSchedule);
}
