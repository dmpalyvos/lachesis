package io.palyvos.scheduler.policy.translators.concrete.normalizers;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalDouble;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LogDecisionNormalizer implements DecisionNormalizer {

  private static final Logger LOG = LogManager.getLogger();
  private final DecisionNormalizer delegate;

  public LogDecisionNormalizer(DecisionNormalizer delegate) {
    Validate.notNull(delegate, "delegate");
    this.delegate = delegate;
  }

  @Override
  public <T> Map<T, Long> normalize(Map<T, Double> schedule) {
    Map<T, Double> logSchedule = new HashMap<>();
    OptionalDouble min = schedule.values().stream().mapToDouble(Double::doubleValue).min();
    Validate.isTrue(min.isPresent(), "Empty schedule!");
    double shift = Math.abs(min.getAsDouble()) + 1; // Necessary shift to move all input values above 1
    for (T key : schedule.keySet()) {
      // Apply the log, using values in the range [1, inf]
      logSchedule.put(key, Math.log(schedule.get(key) + shift));
    }
    return delegate.normalize(logSchedule);
  }

  @Override
  public <T> boolean isValid(Map<T, Double> schedule) {
    return delegate.isValid(schedule);
  }
}
