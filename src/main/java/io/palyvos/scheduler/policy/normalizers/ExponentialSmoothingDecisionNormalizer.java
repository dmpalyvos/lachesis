package io.palyvos.scheduler.policy.normalizers;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExponentialSmoothingDecisionNormalizer implements DecisionNormalizer {

  private static final Logger LOG = LogManager.getLogger();
  private final Map<Object, Double> smoothSchedule = new HashMap<>();
  private final DecisionNormalizer delegate;
  private final double alpha;

  public ExponentialSmoothingDecisionNormalizer(DecisionNormalizer delegate,
      double smoothingFactor) {
    Validate.notNull(delegate, "delegate");
    this.delegate = delegate;
    Validate.isTrue(smoothingFactor >= 0 && smoothingFactor <= 1,
        "smoothingFactor must be in [0, 1]: %f", smoothingFactor);
    this.alpha = smoothingFactor;
  }

  @Override
  public <T> Map<T, Long> normalize(Map<T, Double> schedule) {
    // Some ugly (but safe) casts due to the way the API is designed...
    for (T key : schedule.keySet()) {
      smoothSchedule.put(key, ewma(smoothSchedule.get(key), schedule.get(key)));
    }
    return delegate.normalize((Map<T, Double>) smoothSchedule);
  }

  private Double ewma(Double previous, Double current) {
    if (previous == null) {
      return current;
    }
    return alpha * current + (1 - alpha) * previous;
  }

  @Override
  public <T> boolean isValid(Map<T, Double> schedule) {
    return delegate.isValid(schedule);
  }
}
