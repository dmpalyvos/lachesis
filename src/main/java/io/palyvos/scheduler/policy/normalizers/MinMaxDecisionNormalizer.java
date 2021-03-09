package io.palyvos.scheduler.policy.normalizers;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.Validate;

public final class MinMaxDecisionNormalizer implements DecisionNormalizer {

  public static final String NAME = "minmax";
  private final Range targetRange;
  private final boolean force;

  public MinMaxDecisionNormalizer(long min, long max, boolean force) {
    // min > max is allowed if the context requires it
    Validate.isTrue(min != max, "min == max!");
    this.targetRange = new Range(min, max);
    this.force = force;
  }

  public MinMaxDecisionNormalizer(long min, long max) {
    //FIXME: Only for backward compatibility, remove
    this(min, max, true);
  }

  @Override
  public <T> Map<T, Long> normalize(Map<T, Double> schedule) {
    Validate.notEmpty(schedule, "Empty schedule!");
    final Range inputRange = Range.of(schedule.values());
    final boolean scalingRequired = scalingRequired(inputRange);
    Map<T, Long> normalizedSchedulingDecisions = new HashMap<>();
    for (Map.Entry<T, Double> decision : schedule.entrySet()) {
      long normalized = scalingRequired ? normalize(decision.getValue(), inputRange)
          : Math.round(decision.getValue());
      normalizedSchedulingDecisions
          .put(decision.getKey(), normalized);
    }
    return normalizedSchedulingDecisions;
  }

  private boolean scalingRequired(Range inputRange) {
    return force || !targetRange.contains(inputRange);
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

  long normalize(double value, Range inputRange) {
    if (inputRange.max - inputRange.min == 0) {
      return Math.round(targetRange.max);
    }
    return Math.round(
        ((value - inputRange.min) *
            (targetRange.max - targetRange.min) / (inputRange.max - inputRange.min))
            + targetRange.min);

  }

}
