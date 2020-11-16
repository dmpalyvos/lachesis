package io.palyvos.scheduler.policy.translators.concrete.normalizers;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.Validate;

public class IdentityDecisionNormalizer implements DecisionNormalizer {


  @Override
  public <T> Map<T, Long> normalize(Map<T, Double> schedule) {
    Validate.notEmpty(schedule, "Empty schedule!");
    Map<T, Long> normalized = new HashMap<>();
    for (Entry<T, Double> entry : schedule.entrySet()) {
      T key = entry.getKey();
      Double value = entry.getValue();
      normalized.put(key, Math.round(value));
    }
    return normalized;
  }

  @Override
  public <T> boolean isValid(Map<T, Double> schedule) {
    return true;
  }
}
