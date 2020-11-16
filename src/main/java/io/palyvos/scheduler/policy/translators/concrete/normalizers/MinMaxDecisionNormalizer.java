package io.palyvos.scheduler.policy.translators.concrete.normalizers;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.Validate;

public final class MinMaxDecisionNormalizer implements DecisionNormalizer {

  private static final double VALUES_EQUAL_LIMIT = 1E-6;
  private final Range targetRange;

  public MinMaxDecisionNormalizer(long min, long max) {
    // min > max is allowed if the context requires it
    Validate.isTrue(min != max, "min == max!");
    this.targetRange = new Range(min, max);
  }

  @Override
  public <T> Map<T, Long> normalize(Map<T, Double> schedule) {
    Validate.notEmpty(schedule, "Empty schedule!");
    final Range oldRange = range(schedule.values());
    Map<T, Long> normalizedSchedulingDecisions = new HashMap<>();
    for (Map.Entry<T, Double> decision : schedule.entrySet()) {
      normalizedSchedulingDecisions
          .put(decision.getKey(), normalize(decision.getValue(), oldRange));
    }
    return normalizedSchedulingDecisions;
  }

  @Override
  public <T> boolean isValid(Map<T, Double> schedule) {
    for (Double value : schedule.values()) {
      if (value == null || !Double.isFinite(value)) {
        return false;
      }
    }
    return true;
  }

  long normalize(double value, Range oldRange) {
    if (oldRange.max - oldRange.min == 0) {
      return Math.round(targetRange.max);
    }
    return Math.round(
        ((value - oldRange.min) *
            (targetRange.max - targetRange.min) / (oldRange.max - oldRange.min))
            + targetRange.min);

  }

  Range range(Collection<Double> values) {
    boolean first = true;
    double min = 0;
    double max = 0;
    for (Double value : values) {
      if (first) {
        min = max = value;
        first = false;
        continue;
      }
      if (value > max) {
        max = value;
      }
      if (value < min) {
        min = value;
      }
    }
    return new Range(min, max);
  }

}
