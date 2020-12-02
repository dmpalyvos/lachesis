package io.palyvos.scheduler.adapters.flink;

import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.metric.graphite.GraphiteMetricReport;
import io.palyvos.scheduler.task.Operator;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.DoubleBinaryOperator;
import java.util.function.Function;

public enum FlinkGraphiteMetric implements Metric<FlinkGraphiteMetric> {
  SUBTASK_TUPLES_IN_RECENT("groupByNode(*.taskmanager.*.*.*.*.numRecordsInPerSecond.m1_rate, 4, 'avg')",
      SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, (operators, metrics) ->
      metrics.get(operators.get(0).id())),
  SUBTASK_TUPLES_OUT_RECENT("groupByNode(*.taskmanager.*.*.*.*.numRecordsOutPerSecond.m1_rate, 4, 'avg')",
      SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, (operators, metrics) -> metrics
      .get(operators.get(operators.size() - 1).id())),
  SUBTASK_TUPLES_IN_TOTAL("groupByNode(*.taskmanager.*.*.*.*.numRecordsIn.count, 4, 'avg')",
      SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, (operators, metrics) ->
      metrics.get(operators.get(0).id())),
  SUBTASK_TUPLES_OUT_TOTAL("groupByNode(*.taskmanager.*.*.*.*.numRecordsOut.count, 4, 'avg')",
      SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, (operators, metrics) -> metrics
      .get(operators.get(operators.size() - 1).id()));

  private final String graphiteQuery;
  private final int window;
  private final BiFunction<List<Operator>, Map<String, Double>, Double> operatorFunction;

  FlinkGraphiteMetric(String graphiteQuery, int window,
      BiFunction<List<Operator>, Map<String, Double>, Double> operatorFunction) {
    this.graphiteQuery = graphiteQuery;
    this.window = window;
    this.operatorFunction = operatorFunction;
  }

  public void compute(FlinkGraphiteMetricProvider flinkGraphiteMetricProvider) {
    //FIXME: Reevaluate window and reduce function after selecting exact metrics
    Map<String, Double> operatorMetricValues = flinkGraphiteMetricProvider
        .fetchFromGraphite(graphiteQuery, window, report -> report.reduce(0, (a, b) -> b));
    Map<String, Double> taskMetricValues = new HashMap<>();
    //FIXME: This will give inaccurate results in case of chaining with multiple outputs
    //e.g. SOURCE -> (FILTER1, FILTER2)
    flinkGraphiteMetricProvider.traverser.forEachTaskFromSourceBFS(task -> {
      taskMetricValues
          .put(task.id(), operatorFunction.apply(task.operators(), operatorMetricValues));
    });
    flinkGraphiteMetricProvider.replaceMetricValues(this, taskMetricValues);
  }
}
