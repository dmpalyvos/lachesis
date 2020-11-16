package io.palyvos.scheduler.metric;

import java.util.HashMap;
import java.util.Map;

class MockMetricProvider extends AbstractMetricProvider<MockMetric> {

  static final Map<Metric, MockMetric> METRIC_MAPPING = new HashMap<>();

  static {
    for (BaseSchedulerMetric metric : BaseSchedulerMetric.values()) {
      if (metric.isInternal()) {
        continue;
      }
      try {
        METRIC_MAPPING.put(metric, MockMetric.valueOf(metric.name()));
      } catch (Exception e) {
      }
    }
  }

  public MockMetricProvider() {
    super(METRIC_MAPPING, MockMetric.class);
  }

  @Override
  protected void doCompute(MockMetric metric) {

  }
}
