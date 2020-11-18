package io.palyvos.scheduler.metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test {@link io.palyvos.scheduler.metric.AbstractMetricProvider} base methods through
 * TestMetricProvider (which is basically a noop implementation).
 */
@Test
public class AbstractMetricProviderTest {

  private enum TestMetric implements Metric<TestMetric> {
    IN_TOTAL_DEPENDENCY_1,
    IN_TOTAL_DEPENDENCY_2,
    SUBTASK_TUPLES_IN_TOTAL {
      @Override
      public Set<TestMetric> dependencies() {
        return Stream.of(IN_TOTAL_DEPENDENCY_1, IN_TOTAL_DEPENDENCY_2).collect(Collectors.toSet());
      }
    },
  }

  private static class TestMetricProvider extends AbstractMetricProvider<TestMetric> {

    public TestMetricProvider() {
      super(mappingFor(TestMetric.values()), TestMetric.class);
    }

    @Override
    protected void doCompute(TestMetric metric) {

    }
  }

  /**
   * Internally in {@link io.palyvos.scheduler.metric.SchedulerMetricProvider}, the generics of
   * delegate {@link io.palyvos.scheduler.metric.MetricProvider}s are lost. Verify that the runtime
   * checks catch wrong type issues
   */
  @Test(expectedExceptions = {IllegalArgumentException.class})
  void registerWrongClass() {
    AbstractMetricProvider provider = new TestMetricProvider();
    provider.register(BasicSchedulerMetric.SUBTASK_TUPLES_IN_TOTAL);
  }

  @Test(expectedExceptions = {NullPointerException.class})
  void registerNull() {
    AbstractMetricProvider provider = new TestMetricProvider();
    provider.register(null);
  }

  @Test
  void registerWithDependencies() {
    AbstractMetricProvider provider = new TestMetricProvider();
    provider.register(TestMetric.SUBTASK_TUPLES_IN_TOTAL);
    Assert.assertTrue(provider.registeredMetrics.contains(TestMetric.SUBTASK_TUPLES_IN_TOTAL),
        "main metric");
    TestMetric.SUBTASK_TUPLES_IN_TOTAL.dependencies().forEach(dependency ->
        Assert.assertTrue(provider.registeredMetrics.contains(dependency), "dependency"));
  }

  @Test
  void canProvide() {
    AbstractMetricProvider provider = new TestMetricProvider();
    provider.register(TestMetric.SUBTASK_TUPLES_IN_TOTAL);
    Assert
        .assertTrue(provider.canProvide(BasicSchedulerMetric.SUBTASK_TUPLES_IN_TOTAL), "canProvide");
  }

  @Test
  void toProvidedMetric() {
    AbstractMetricProvider provider = new TestMetricProvider();
    // Based on the equal name mapping
    Assert.assertEquals(provider.toProvidedMetric(BasicSchedulerMetric.SUBTASK_TUPLES_IN_TOTAL),
        TestMetric.SUBTASK_TUPLES_IN_TOTAL);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  void toProvidedMetricNonRegistered() {
    AbstractMetricProvider provider = new TestMetricProvider();
    // Based on the equal name mapping
    provider.toProvidedMetric(BasicSchedulerMetric.SUBTASK_COST);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  void getWrongClass() {
    AbstractMetricProvider provider = new TestMetricProvider();
    provider.get(BasicSchedulerMetric.SUBTASK_TUPLES_IN_TOTAL);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  void replaceMetricValuesWrongClass() {
    AbstractMetricProvider provider = new TestMetricProvider();
    provider.register(TestMetric.SUBTASK_TUPLES_IN_TOTAL);
    Map<String, Double> values = new HashMap<>();
    values.put("test", 0.1);
    provider.replaceMetricValues(BasicSchedulerMetric.SUBTASK_TUPLES_IN_TOTAL, values);
  }

  @Test
  void replaceMetricValues() {
    AbstractMetricProvider provider = new TestMetricProvider();
    provider.register(TestMetric.SUBTASK_TUPLES_IN_TOTAL);
    Map<String, Double> values = new HashMap<>();
    values.put("test", 0.1);
    provider.replaceMetricValues(TestMetric.SUBTASK_TUPLES_IN_TOTAL, values);
    Assert.assertEquals(provider.get(TestMetric.SUBTASK_TUPLES_IN_TOTAL), values, "values");
  }

  @Test
  void mergeMetricValuesSum() {
    AbstractMetricProvider provider = new TestMetricProvider();
    provider.register(TestMetric.SUBTASK_TUPLES_IN_TOTAL);
    Map<String, Double> values1 = new HashMap<>();
    values1.put("test1", 1.0);
    Map<String, Double> values2 = new HashMap<>();
    values2.put("test1", 1.0);
    values2.put("test2", 1.0);
    Map<String, Double> expectedValues = new HashMap<>();
    expectedValues.put("test1", 1.0 + 1.0);
    expectedValues.put("test2", 1.0);
    provider.replaceMetricValues(TestMetric.SUBTASK_TUPLES_IN_TOTAL, values1);
    provider
        .mergeMetricValues(TestMetric.SUBTASK_TUPLES_IN_TOTAL, values2, AbstractMetricProvider.SUM);
    Assert.assertEquals(provider.get(TestMetric.SUBTASK_TUPLES_IN_TOTAL), expectedValues, "values");
  }

  @Test
  void mergeMetricValuesNonExisting() {
    AbstractMetricProvider provider = new TestMetricProvider();
    provider.register(TestMetric.SUBTASK_TUPLES_IN_TOTAL);
    Map<String, Double> values1 = new HashMap<>();
    values1.put("test1", 1.0);
    values1.put("test2", 1.0);
    Map<String, Double> expectedValues = new HashMap<>();
    expectedValues.put("test1", 1.0);
    expectedValues.put("test2", 1.0);
    provider
        .mergeMetricValues(TestMetric.SUBTASK_TUPLES_IN_TOTAL, values1, AbstractMetricProvider.SUM);
    Assert.assertEquals(provider.get(TestMetric.SUBTASK_TUPLES_IN_TOTAL), expectedValues, "values");
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  void getNonRegistered() {
    AbstractMetricProvider provider = new TestMetricProvider();
    provider.get(TestMetric.SUBTASK_TUPLES_IN_TOTAL);
  }

  @Test
  void getNonComputed() {
    AbstractMetricProvider provider = new TestMetricProvider();
    provider.register(TestMetric.SUBTASK_TUPLES_IN_TOTAL);
    Assert.assertNull(provider.get(TestMetric.SUBTASK_TUPLES_IN_TOTAL), "metric data");
  }

  @Test
  void computeWithDependencies() {
    final List<TestMetric> computeLog = new ArrayList<>();
    AbstractMetricProvider provider = new TestMetricProvider() {
      @Override
      protected void doCompute(TestMetric metric) {
        computeLog.add(metric);
      }
    };
    provider.register(TestMetric.SUBTASK_TUPLES_IN_TOTAL);
    provider.compute(TestMetric.SUBTASK_TUPLES_IN_TOTAL);
    final List<TestMetric> expectedComputeLog = new ArrayList<>();
    expectedComputeLog.add(TestMetric.SUBTASK_TUPLES_IN_TOTAL);
    expectedComputeLog.addAll(TestMetric.SUBTASK_TUPLES_IN_TOTAL.dependencies());
    Collections.sort(expectedComputeLog);
    Collections.sort(computeLog);
    Assert.assertEquals(computeLog, expectedComputeLog, "compute log");
  }

  @Test
  void noDuplicateCompute() {
    final List<TestMetric> computeLog = new ArrayList<>();
    AbstractMetricProvider provider = new TestMetricProvider() {
      @Override
      protected void doCompute(TestMetric metric) {
        computeLog.add(metric);
      }
    };
    provider.register(TestMetric.SUBTASK_TUPLES_IN_TOTAL);
    provider.compute(TestMetric.SUBTASK_TUPLES_IN_TOTAL);
    provider.compute(TestMetric.SUBTASK_TUPLES_IN_TOTAL);
    final List<TestMetric> expectedComputeLog = new ArrayList<>();
    expectedComputeLog.add(TestMetric.SUBTASK_TUPLES_IN_TOTAL);
    expectedComputeLog.addAll(TestMetric.SUBTASK_TUPLES_IN_TOTAL.dependencies());
    Collections.sort(expectedComputeLog);
    Collections.sort(computeLog);
    Assert.assertEquals(computeLog, expectedComputeLog, "compute log");
  }


}
