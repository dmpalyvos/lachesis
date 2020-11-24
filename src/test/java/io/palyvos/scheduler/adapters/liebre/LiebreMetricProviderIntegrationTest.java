package io.palyvos.scheduler.adapters.liebre;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * Dirty integration testing/experimentation, do not include in suite.
 */
@Test
public class LiebreMetricProviderIntegrationTest {

  @Test
  void totalTuplesTest() {
    LiebreMetricProvider provider = new LiebreMetricProvider("129.16.20.158", 80, "liebre.OS2");
    provider.register(LiebreMetric.SUBTASK_TUPLES_IN_RECENT);
    provider.register(LiebreMetric.SUBTASK_TUPLES_OUT_RECENT);
    provider.run();
    Map<String, Double> inTotal = provider.get(LiebreMetric.SUBTASK_TUPLES_IN_RECENT);
    Map<String, Double> outTotal = provider.get(LiebreMetric.SUBTASK_TUPLES_OUT_RECENT);
    Set<String> allTasks = new HashSet<>();
    allTasks.addAll(inTotal.keySet());
    allTasks.addAll(outTotal.keySet());
    allTasks.stream().sorted(String::compareTo)
        .forEach(task -> {
          Double in = inTotal.get(task);
          Double out = outTotal.get(task);
          System.out.format("%s %s %s\n", task, in, out);
        });
  }

}
