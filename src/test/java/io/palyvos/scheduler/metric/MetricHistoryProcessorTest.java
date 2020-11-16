package io.palyvos.scheduler.metric;

import io.palyvos.scheduler.metric.MetricHistoryProcessor.WindowLatestMinusEarliestFunction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetricHistoryProcessorTest {


  @Test(expectedExceptions = {NullPointerException.class})
  public void putNull() {
    newMetricHistoryProcessor(1, MockMetric.SUBTASK_TUPLES_IN_TOTAL)
        .add(MockMetric.SUBTASK_TUPLES_IN_TOTAL, null, 1);
  }

  @Test
  public void putEmpty() {
    newMetricHistoryProcessor(1, MockMetric.SUBTASK_TUPLES_IN_TOTAL)
        .add(MockMetric.SUBTASK_TUPLES_IN_TOTAL, Collections.emptyMap(), 1);
    // No exception
  }

  @Test
  public void emptyWindow() {
    final MockMetric metric = MockMetric.SUBTASK_TUPLES_IN_TOTAL;
    MetricHistoryProcessor<MockMetric> processor = newMetricHistoryProcessor(2, metric);
    Map<String, Double> resultFromCurrentTs = processor.process(metric, 0);
    Assert.assertEquals(resultFromCurrentTs.size(), 0, "not-empty");
  }

  @Test
  public void singleValueWindow() {
    final MockMetric metric = MockMetric.SUBTASK_TUPLES_IN_TOTAL;
    MetricHistoryProcessor<MockMetric> processor = newMetricHistoryProcessor(2, metric);
    Map<String, Double> metricValues = dummyMetricValues(0);
    processor.add(metric, metricValues, 0);
    Map<String, Double> resultFromCurrentTs = processor.process(metric, 0);
    Assert.assertEquals(resultFromCurrentTs, metricValues, "not-empty");
  }

  @Test
  public void partialWindow() {
    final MockMetric metric = MockMetric.SUBTASK_TUPLES_IN_TOTAL;
    MetricHistoryProcessor<MockMetric> processor = newMetricHistoryProcessor(10, metric);
    processor.add(metric, dummyMetricValues(0), 0);
    for (int ts = 1; ts < 5; ts++) {
      processor.add(metric, dummyMetricValues(ts), ts);
      Map<String, Double> resultFromCurrentTs = processor.process(metric, ts);
      Assert.assertEquals((double) resultFromCurrentTs.get("A"), ts - 0, "A-diff-curr");
      Assert.assertEquals((double) resultFromCurrentTs.get("B"), ts - 0, "B-diff-curr");
    }
  }

  @Test
  public void fullWindow() {
    final MockMetric metric = MockMetric.SUBTASK_TUPLES_IN_TOTAL;
    MetricHistoryProcessor<MockMetric> processor = newMetricHistoryProcessor(2, metric);
    for (int ts = 0; ts < 3; ts++) {
      processor.add(metric, dummyMetricValues(ts), ts);
    }
    Map<String, Double> resultFromCurrentTs = processor.process(metric, 2);
    Assert.assertEquals((double) resultFromCurrentTs.get("A"), 2.0, "A-diff-curr");
    Assert.assertEquals((double) resultFromCurrentTs.get("B"), 2.0, "B-diff-curr");
    Map<String, Double> resultFromLaterTs = processor.process(metric, 3);
    Assert.assertEquals((double) resultFromLaterTs.get("A"), 2.0, "A-diff-later");
    Assert.assertEquals((double) resultFromLaterTs.get("B"), 2.0, "B-diff-later");
  }

  @Test
  public void fullWindowWithCleanup() {
    final MockMetric metric = MockMetric.SUBTASK_TUPLES_IN_TOTAL;
    MetricHistoryProcessor<MockMetric> processor = newMetricHistoryProcessor(2, metric);
    for (int ts = 0; ts < 3; ts++) {
      processor.cleanup(ts);
      processor.add(metric, dummyMetricValues(ts), ts);
    }
    Map<String, Double> resultFromCurrentTs = processor.process(metric, 2);
    Assert.assertEquals((double) resultFromCurrentTs.get("A"), 2.0, "A-diff-curr");
    Assert.assertEquals((double) resultFromCurrentTs.get("B"), 2.0, "B-diff-curr");
    Map<String, Double> resultFromLaterTs = processor.process(metric, 3);
    Assert.assertEquals((double) resultFromLaterTs.get("A"), 2.0, "A-diff-later");
    Assert.assertEquals((double) resultFromLaterTs.get("B"), 2.0, "B-diff-later");
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void nonRegisteredMetric() {
    MetricHistoryProcessor<MockMetric> processor = newMetricHistoryProcessor(2,
        MockMetric.SUBTASK_TUPLES_IN_TOTAL);
    processor.add(MockMetric.SUBTASK_TUPLES_OUT_TOTAL, Collections.emptyMap(), 0);
  }

  @Test(expectedExceptions = {IllegalStateException.class})
  public void overwriteTimestamp() {
    final MockMetric metric = MockMetric.SUBTASK_TUPLES_IN_TOTAL;
    MetricHistoryProcessor<MockMetric> processor = newMetricHistoryProcessor(2, metric);
    processor.add(metric, Collections.emptyMap(), 0);
    processor.add(metric, Collections.emptyMap(), 0);
  }

  private MetricHistoryProcessor<MockMetric> newMetricHistoryProcessor(long windowSize,
      MockMetric... metrics) {
    return new MetricHistoryProcessor<>(windowSize, WindowLatestMinusEarliestFunction.INSTANCE,
        metrics);
  }

  private Map<String, Double> dummyMetricValues(int diff) {
    Map<String, Double> values = new HashMap<>();
    values.put("A", 100.0 + diff);
    values.put("B", 50.0 + diff);
    return values;
  }
}
