package io.palyvos.scheduler.adapters.storm;

import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.util.SchedulerContext;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public enum StormGraphiteMetric implements Metric<StormGraphiteMetric> {
  TASK_QUEUE_SIZE_FROM_SUBTASK_DATA(
      "groupByNode(Storm.*.%s.*.*.*.receive.population.value, %d, 'avg')"),
  TASK_OUTPUT_QUEUE_SIZE_FROM_SUBTASK_DATA("groupByNode(Storm.*.%s.*.*.*.sendqueue.population.value, %d, 'avg')"),
  SUBTASK_TUPLES_IN_RECENT("groupByNode(Storm.*.%s.*.*.*.execute-count.*.value, %d, 'avg')"),
  SUBTASK_TUPLES_OUT_RECENT("groupByNode(Storm.*.%s.*.*.*.transfer-count.*.value, %d, 'avg')");

  private final String graphiteQuery;
  private final int operatorBaseIndex = 3;

  StormGraphiteMetric(String graphiteQuery) {
    //query format: Storm.jobName.[hostname-part]+.worker.node.instance...
    try {
      String localHostname = InetAddress.getLocalHost().getCanonicalHostName();
      int hostnamePartsNumber = localHostname.split("\\.").length;
      // Keep only tasks that are running in this host
      this.graphiteQuery = String
          .format(graphiteQuery, localHostname, operatorBaseIndex + hostnamePartsNumber);
    } catch (UnknownHostException e) {
      throw new IllegalStateException(
          String.format("Hostname not defined correctly in this machine: %s", e.getMessage()));
    }
  }


  public void compute(StormGraphiteMetricProvider stormGraphiteMetricProvider) {
    //FIXME: Adjust default window size and default value depending on metric
    Map<String, Double> metricValues = stormGraphiteMetricProvider
        .fetchFromGraphite(graphiteQuery, SchedulerContext.METRIC_RECENT_PERIOD_SECONDS,
            report -> report.average(0));
    stormGraphiteMetricProvider.replaceMetricValues(this, metricValues);
  }
}
