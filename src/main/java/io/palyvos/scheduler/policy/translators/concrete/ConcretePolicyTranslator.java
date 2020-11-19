package io.palyvos.scheduler.policy.translators.concrete;

import io.palyvos.scheduler.task.ExternalThread;
import java.util.Map;

public interface ConcretePolicyTranslator {

  void applyPolicy(Map<ExternalThread, Double> schedule);

  int applyDirect(Map<ExternalThread, Long> normalizedSchedule);
}
