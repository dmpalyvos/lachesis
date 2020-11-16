package io.palyvos.scheduler.policy.translators.concrete.normalizers;

import java.util.Map;

public interface DecisionNormalizer {

  <T> Map<T, Long> normalize(Map<T, Double> schedule);

  <T> boolean isValid(Map<T, Double> schedule);
}
