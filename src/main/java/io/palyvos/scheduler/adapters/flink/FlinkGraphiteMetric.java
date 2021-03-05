package io.palyvos.scheduler.adapters.flink;

import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.metric.graphite.GraphiteMetricReport;
import io.palyvos.scheduler.task.Operator;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.util.SchedulerContext;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public enum FlinkGraphiteMetric implements Metric<FlinkGraphiteMetric> {
  SUBTASK_TUPLES_IN_RECENT(
      "groupByNode(%s.taskmanager.*.*.*.*.numRecordsInPerSecond.m1_rate, 4, 'avg')",
      SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, Task::headOperators,
      report -> report.reduce(0, (a, b) -> b)),
  SUBTASK_TUPLES_OUT_RECENT(
      "groupByNode(%s.taskmanager.*.*.*.*.numRecordsOutPerSecond.m1_rate, 4, 'avg')",
      SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, Task::tailOperators,
      report -> report.reduce(0, (a, b) -> b)),
  TASK_QUEUE_SIZE_FROM_SUBTASK_DATA(
      "groupByNode(%s.taskmanager.*.*.*.*.Shuffle.Netty.Input.Buffers.inputQueueLength, 4, 'avg')",
      SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, Task::headOperators,
      report -> report.average(0));

  protected final String graphiteQuery;
  protected final int window;
  protected final Function<Task, Collection<Operator>> operatorFunction;
  // NOTE: Flink reports tasks metrics directly, so reduceFunction is ignored for now!
  protected final Function<GraphiteMetricReport, Double> reportReduceFunction;

  FlinkGraphiteMetric(String graphiteQuery, int window,
      Function<Task, Collection<Operator>> operatorFunction,
      Function<GraphiteMetricReport, Double> reportReduceFunction) {
    this.graphiteQuery = onlyLocalTasks(graphiteQuery);
    this.window = window;
    this.operatorFunction = operatorFunction;
    this.reportReduceFunction = reportReduceFunction;
  }

  private String onlyLocalTasks(String query) {
    // First part of the metric is the IP, we only want the metrics of tasks in this machine
    try {
      // Replace dots with dashes in graphite, to match the flink key format
      final String localIp = Inet4Address.getLocalHost().getHostAddress().replace(".", "-");
      return String.format(query, localIp);
    } catch (UnknownHostException e) {
      throw new IllegalStateException(String.format("Failed to retrieve local IP: %s", e));
    }
  }

  public void compute(FlinkGraphiteMetricProvider provider) {
    Map<String, Double> operatorMetricValues = provider
        .fetchFromGraphite(graphiteQuery, window, reportReduceFunction);
    Map<String, Double> taskMetricValues = new HashMap<>();
    provider.traverser.forEachTaskFromSourceBFS(task -> {
      taskMetricValues.put(task.id(), operatorMetricValues.get(task.id().replace(" ", "-")));
    });
    provider.replaceMetricValues(this, taskMetricValues);
  }
}
