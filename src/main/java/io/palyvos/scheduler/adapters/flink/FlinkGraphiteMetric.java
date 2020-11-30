package io.palyvos.scheduler.adapters.flink;

import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Map;

public enum FlinkGraphiteMetric implements Metric<FlinkGraphiteMetric> {
  SUBTASK_TUPLES_IN_TOTAL("groupByNode(*.taskmanager.*.*.*.*.numRecordsIn.count, 4, 'avg')"),
  SUBTASK_TUPLES_OUT_TOTAL("groupByNode(*.taskmanager.*.*.*.*.numRecordsOut.count, 4, 'avg')");

  private final String graphiteQuery;

  FlinkGraphiteMetric(String graphiteQuery) {
    this.graphiteQuery = graphiteQuery;
  }


  public void compute(FlinkGraphiteMeticProvider flinkGraphiteMeticProvider) {
    Map<String, Double> metricValues = flinkGraphiteMeticProvider
        .fetchFromGraphite(graphiteQuery, SchedulerContext.METRIC_RECENT_PERIOD_SECONDS,
            report -> report.average(0));
    flinkGraphiteMeticProvider.replaceMetricValues(this, metricValues);
  }
}
