package io.palyvos.scheduler.policy.translators.concrete.normalizers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LogDecisionNormalizerTest {

  @Test(expectedExceptions = IllegalArgumentException.class)
  void emptySchedule() {
   LogDecisionNormalizer normalizer = new LogDecisionNormalizer(new IdentityDecisionNormalizer());
   normalizer.normalize(Collections.emptyMap());
  }

  @Test
  void oneValue() {
    LogDecisionNormalizer normalizer = new LogDecisionNormalizer(new IdentityDecisionNormalizer());
    Map<String, Double> schedule = new HashMap<>();
    schedule.put("A", 10.0);
    Map<String, Long> normalized = normalizer.normalize(schedule);
    Assert.assertEquals((long) normalized.get("A"), 3);
  }

  @Test
  void twoValues() {
    LogDecisionNormalizer normalizer = new LogDecisionNormalizer(new IdentityDecisionNormalizer());
    Map<String, Double> schedule = new HashMap<>();
    schedule.put("A", 10.0);
    schedule.put("B", 100.0);
    Map<String, Long> normalized = normalizer.normalize(schedule);
    Assert.assertEquals((long)normalized.get("A"), 3);
    Assert.assertEquals((long)normalized.get("B"), 5);
  }

  @Test
  void negativeValue() {
    LogDecisionNormalizer normalizer = new LogDecisionNormalizer(new IdentityDecisionNormalizer());
    Map<String, Double> schedule = new HashMap<>();
    schedule.put("A", -10.0);
    Map<String, Long> normalized = normalizer.normalize(schedule);
    Assert.assertEquals((long)normalized.get("A"), 0);
  }

  @Test
  void negativeAndPositiveValues() {
    LogDecisionNormalizer normalizer = new LogDecisionNormalizer(new IdentityDecisionNormalizer());
    Map<String, Double> schedule = new HashMap<>();
    schedule.put("A", -1000.0);
    schedule.put("B", -10.0);
    schedule.put("C", 1000.0);
    schedule.put("D", 10000.0);
    Map<String, Long> normalized = normalizer.normalize(schedule);
    Assert.assertEquals((long)normalized.get("A"), 0);
    Assert.assertEquals((long)normalized.get("B"), 7);
    Assert.assertEquals((long)normalized.get("C"), 8);
    Assert.assertEquals((long)normalized.get("D"), 9);
  }

}
