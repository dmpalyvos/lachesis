package io.palyvos.scheduler.policy.normalizers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IdentityDecisionNormalizerTest {

  @Test(expectedExceptions = IllegalArgumentException.class)
  void emptySchedule() {
   IdentityDecisionNormalizer normalizer = new IdentityDecisionNormalizer();
   normalizer.normalize(Collections.emptyMap());
  }

  @Test
  void normalizeTest() {
    IdentityDecisionNormalizer normalizer = new IdentityDecisionNormalizer();
    Random random = new Random(0);
    Map<String, Double> schedule = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      schedule.put(String.valueOf(i), 100*random.nextDouble());
    }
    Map<String, Long> normalized = normalizer.normalize(schedule);
    for (int i = 0; i < 10; i++) {
      String key = String.valueOf(i);
      Assert.assertEquals((long) normalized.get(key), Math.round(schedule.get(key)), key);
    }
  }

}
