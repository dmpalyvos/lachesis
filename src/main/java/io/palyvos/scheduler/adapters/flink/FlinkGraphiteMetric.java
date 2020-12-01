package io.palyvos.scheduler.adapters.flink;

import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.task.Operator;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.HashMap;
import java.util.Map;

public enum FlinkGraphiteMetric implements Metric<FlinkGraphiteMetric> {
  SUBTASK_TUPLES_IN_RECENT("groupByNode(*.taskmanager.*.*.*.*.numRecordsIn.count, 4, 'avg')", SchedulerContext.METRIC_RECENT_PERIOD_SECONDS),
  SUBTASK_TUPLES_OUT_RECENT("groupByNode(*.taskmanager.*.*.*.*.numRecordsOut.count, 4, 'avg')", SchedulerContext.METRIC_RECENT_PERIOD_SECONDS),
  SUBTASK_TUPLES_IN_TOTAL("groupByNode(*.taskmanager.*.*.*.*.numRecordsIn.count, 4, 'avg')", SchedulerContext.METRIC_TOTAL_PERIOD_SECONDS),
  SUBTASK_TUPLES_OUT_TOTAL("groupByNode(*.taskmanager.*.*.*.*.numRecordsOut.count, 4, 'avg')", SchedulerContext.METRIC_TOTAL_PERIOD_SECONDS);

  private final String graphiteQuery;
  private final int window;

  FlinkGraphiteMetric(String graphiteQuery, int window) {
    this.graphiteQuery = graphiteQuery;
    this.window = window;
  }


  public void compute(FlinkGraphiteMetricProvider flinkGraphiteMetricProvider) {
    Map<String, Double> operatorMetricValues = flinkGraphiteMetricProvider
        .fetchFromGraphite(graphiteQuery, window,
            report -> report.average(0));
    Map<String, Double> taskMetricValues = new HashMap<>();
    flinkGraphiteMetricProvider.traverser.forEachTaskFromSourceBFS(task -> {
      //FIXME: Type of operator function (avg, sum, max, etc) should depend on metric
      double sum = 0;
      long count = 0;
      for (Operator operator : task.operators()) {
        Double aDouble = operatorMetricValues.get(operator.id());
        if (aDouble != null) {
          double doubleValue = aDouble.doubleValue();
          sum += doubleValue;
          count++;
        }
      }
      double operatorAverage = count > 0 ? sum / count : 0;
      taskMetricValues.put(task.id(), operatorAverage);
    });
    flinkGraphiteMetricProvider.replaceMetricValues(this, taskMetricValues);
  }
}
