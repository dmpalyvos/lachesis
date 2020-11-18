package io.palyvos.scheduler.metric;

import java.util.HashMap;
import java.util.Map;

class MockMetricProvider extends AbstractMetricProvider<MockMetric> {

  public MockMetricProvider() {
    super(mappingFor(MockMetric.values()), MockMetric.class);
  }

  @Override
  protected void doCompute(MockMetric metric) {

  }
}
