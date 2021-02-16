package io.palyvos.scheduler.adapters.liebre;

import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.metric.graphite.GraphiteMetricReport;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.Validate;

enum LiebreMetric implements Metric<LiebreMetric> {
  SUBTASK_TUPLES_IN_RECENT {
    @Override
    protected void compute(LiebreMetricProvider provider) {
      final Map<String, Double> streamReads = provider
          .fetchFromGraphite(
              groupByNode(movingAverage(graphiteQuery("OUT.count"),
                  SchedulerContext.METRIC_RECENT_PERIOD_SECONDS), "avg"),
              SchedulerContext.METRIC_RECENT_PERIOD_SECONDS + 1,
              GraphiteMetricReport::last);
      final Map<String, Double> subtaskIn = new HashMap<>();
      for (String stream : streamReads.keySet()) {
        String[] operators = stream.split("_");
        Validate.validState(operators.length == 2, "Invalid stream name: %s", stream);
        String reader = operators[1];
        subtaskIn.put(reader, streamReads.get(stream));
      }
      provider.replaceMetricValues(SUBTASK_TUPLES_IN_RECENT, subtaskIn);
    }
  },
  SUBTASK_TUPLES_OUT_RECENT {
    @Override
    protected void compute(LiebreMetricProvider provider) {
      final Map<String, Double> subtaskOut = new HashMap<>();
      final Map<String, Double> streamWrites = provider
          .fetchFromGraphite(
              groupByNode(movingAverage(graphiteQuery("IN.count"),
                  SchedulerContext.METRIC_RECENT_PERIOD_SECONDS), "avg"),
              SchedulerContext.METRIC_RECENT_PERIOD_SECONDS + 1,
              GraphiteMetricReport::last);
      for (String stream : streamWrites.keySet()) {
        String[] operators = stream.split("_");
        Validate.validState(operators.length == 2, "Invalid stream name: %s", stream);
        String writer = operators[0];
        subtaskOut.put(writer, streamWrites.get(stream));
      }
      provider.replaceMetricValues(SUBTASK_TUPLES_OUT_RECENT, subtaskOut);
    }
  },
  TASK_QUEUE_SIZE_FROM_SUBTASK_DATA {
    @Override
    protected void compute(LiebreMetricProvider provider) {
      final Map<String, Double> streamSizes = provider
          .fetchFromGraphite(
              groupByNode(movingAverage(graphiteQuery("QUEUE_SIZE"),
                  SchedulerContext.METRIC_RECENT_PERIOD_SECONDS), "avg"),
              SchedulerContext.METRIC_RECENT_PERIOD_SECONDS + 1,
              GraphiteMetricReport::last);
      final Map<String, Double> operatorQueueSizes = new HashMap<>();
      for (String stream : streamSizes.keySet()) {
        String[] operators = stream.split("_");
        Validate.validState(operators.length == 2, "Invalid stream name: %s", stream);
        String reader = operators[1];
        operatorQueueSizes.put(reader, streamSizes.get(stream));
      }
      //FIXME: Does this make sense?
      double average = operatorQueueSizes.values().stream().filter(Objects::nonNull)
          .mapToDouble(Double::doubleValue).average().orElse(0);
      provider.traverser.sourceTasks().forEach(task -> operatorQueueSizes.put(task.id(), average));
      provider.replaceMetricValues(this, operatorQueueSizes);
    }
  },
  TASK_LATENCY {
    @Override
    protected void compute(LiebreMetricProvider provider) {
      final Map<String, Double> latencies = provider
          .fetchFromGraphite(
              groupByNode(movingAverage(graphiteQuery("ARRIVAL_TIME"),
                  SchedulerContext.METRIC_RECENT_PERIOD_SECONDS), "avg"),
              SchedulerContext.METRIC_RECENT_PERIOD_SECONDS + 1,
              report -> report.average(-1));
      provider.replaceMetricValues(this, latencies);
    }
  };;


  private final Set<LiebreMetric> dependencies;

  LiebreMetric(LiebreMetric... metrics) {
    this.dependencies = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(metrics)));
  }

  @Override
  public Set<LiebreMetric> dependencies() {
    return dependencies;
  }

  protected abstract void compute(LiebreMetricProvider provider);

  private static String graphiteQuery(String key) {
    return String.format("%s.*.%s", LiebreMetricProvider.LIEBRE_METRICS_PREFIX, key);
  }

  private static String groupByNode(String query, String function) {
    return String
        .format("groupByNode(%s, %d, '%s')", query, LiebreMetricProvider.LIEBRE_METRIC_NODE_IDX,
            function);
  }

  private static String movingAverage(String key, int sumWindowSeconds) {
    return String
        .format("movingAverage(%s, '%dsec')", key, sumWindowSeconds);
  }

}
