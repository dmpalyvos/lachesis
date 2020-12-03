package io.palyvos.scheduler.adapters.flink;

import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.metric.graphite.GraphiteMetricReport;
import io.palyvos.scheduler.task.Operator;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public enum FlinkGraphiteMetric implements Metric<FlinkGraphiteMetric> {
  SUBTASK_TUPLES_IN_RECENT(
      "groupByNode(*.taskmanager.*.*.*.*.numRecordsInPerSecond.m1_rate, 4, 'avg')",
      SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, Task::headOperators,
      report -> report.reduce(0, (a, b) -> b)),
  SUBTASK_TUPLES_OUT_RECENT(
      "groupByNode(*.taskmanager.*.*.*.*.numRecordsOutPerSecond.m1_rate, 4, 'avg')",
      SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, Task::tailOperators,
      report -> report.reduce(0, (a, b) -> b)),
  TASK_QUEUE_SIZE_FROM_SUBTASK_DATA(
      "groupByNode(*.taskmanager.*.*.*.*.buffers.inputQueueLength, 4, 'avg')",
      SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, Task::headOperators,
      report -> report.average(0));

  protected final String graphiteQuery;
  protected final int window;
  protected final Function<Task, Collection<Operator>> operatorFunction;
  protected final Function<GraphiteMetricReport, Double> reportReduceFunction;

  FlinkGraphiteMetric(String graphiteQuery, int window,
      Function<Task, Collection<Operator>> operatorFunction,
      Function<GraphiteMetricReport, Double> reportReduceFunction) {
    this.graphiteQuery = graphiteQuery;
    this.window = window;
    this.operatorFunction = operatorFunction;
    this.reportReduceFunction = reportReduceFunction;
  }

  public void compute(FlinkGraphiteMetricProvider provider) {
    Map<String, Double> operatorMetricValues = provider
        .fetchFromGraphite(graphiteQuery, window, reportReduceFunction);
    Map<String, Double> taskMetricValues = new HashMap<>();
    provider.traverser.forEachTaskFromSourceBFS(task -> {
      final double operatorAverage = operatorFunction.apply(task).stream()
          .map(operator -> operatorMetricValues.get(operator.id()))
          .mapToDouble(Double::doubleValue).average().orElse(0);
      taskMetricValues.put(task.id(), operatorAverage);
    });
    provider.replaceMetricValues(this, taskMetricValues);
  }
}
