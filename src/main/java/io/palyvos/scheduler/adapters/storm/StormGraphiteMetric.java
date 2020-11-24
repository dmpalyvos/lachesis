package io.palyvos.scheduler.adapters.storm;

import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Map;

public enum StormGraphiteMetric implements Metric<StormGraphiteMetric> {
  TASK_QUEUE_SIZE_FROM_SUBTASK_DATA(
      "groupByNode(Storm.*.*.*.*.*.receive.population.value, 4, 'sum')"),
  SUBTASK_TUPLES_IN_RECENT("groupByNode(Storm.*.*.*.*.*.execute-count.*.value, 4, 'sum')"),
  SUBTASK_TUPLES_OUT_RECENT("groupByNode(Storm.*.*.*.*.*.transfer-count.*.value, 4, 'sum')");

  private final String graphiteQuery;

  StormGraphiteMetric(String graphiteQuery) {
    this.graphiteQuery = graphiteQuery;
  }


  public void compute(StormGraphiteMetricProvider stormGraphiteMetricProvider) {
    //FIXME: Adjust default window size and default value depending on metric
    Map<String, Double> metricValues = stormGraphiteMetricProvider
        .fetchFromGraphite(graphiteQuery, SchedulerContext.METRIC_RECENT_PERIOD_SECONDS,
            report -> report.average(0));
    stormGraphiteMetricProvider.replaceMetricValues(this, metricValues);
  }
}
