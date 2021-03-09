package io.palyvos.scheduler.policy.normalizers;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.Validate;

public class NiceDecisionNormalizer implements DecisionNormalizer {

  public static final double NICE_PRIORITY_RATIO = 1.25;
  private final MinMaxDecisionNormalizer delegate;
  private final long max;

  public NiceDecisionNormalizer(long min, long max) {
    // min > max always for nice priorities
    Validate.isTrue(min > max, "nice max >= min!");
    // Need to reverse min and max in delegate, otherwise it will reverse the priorities again
    this.delegate = new MinMaxDecisionNormalizer(max, min, false);
    this.max = max;
  }


  @Override
  public <T> Map<T, Long> normalize(Map<T, Double> schedule) {
    Validate.notEmpty(schedule, "Empty schedule!");
    Map<T, Double> nicePriorities = new HashMap<>();
    final Range inputRange = Range.of(schedule.values());
    // Shift input if necessary to prevent negative values
    final double inputShift = inputRange.min < 0 ? Math.abs(inputRange.min) : 0;
    for (Map.Entry<T, Double> decision : schedule.entrySet()) {
      double niceValue = niceValue(decision.getValue(), inputRange.max, inputShift);
      nicePriorities.put(decision.getKey(), niceValue);
    }
    // Do a MinMax normalization if necessary to fit values in the desired range
    return delegate.normalize(nicePriorities);
  }

  double niceValue(double priority, double maxPriority, double inputShift) {
    return max + ((Math.log10(maxPriority + inputShift) - Math.log10(priority + inputShift)) / Math.log10(NICE_PRIORITY_RATIO));
  }

  @Override
  public <T> boolean isValid(Map<T, Double> schedule) {
    return delegate.isValid(schedule);
  }
}
