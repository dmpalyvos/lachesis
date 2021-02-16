package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.task.ExternalThread;
import java.util.Map;

public interface SinglePriorityMetricTranslator {

  void apply(Map<ExternalThread, Double> schedule);

}
