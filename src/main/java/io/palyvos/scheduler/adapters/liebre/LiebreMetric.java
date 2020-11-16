package io.palyvos.scheduler.adapters.liebre;

import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.Validate;

enum LiebreMetric implements Metric<LiebreMetric> {
  SUBTASK_TUPLES_RECENT {
    @Override
    protected void compute(LiebreMetricProvider provider) {
      SubtaskTuplesResult result = subtaskTuplesSum(provider,
          SchedulerContext.METRIC_RECENT_PERIOD_SECONDS);
      provider.replaceMetricValues(SUBTASK_TUPLES_IN_RECENT, result.in);
      provider.replaceMetricValues(SUBTASK_TUPLES_OUT_RECENT, result.out);
    }
  },
  SUBTASK_TUPLES_IN_RECENT(SUBTASK_TUPLES_RECENT) {
    @Override
    protected void compute(LiebreMetricProvider provider) {
      //Computed at SUBTASK_TUPLES_RECENT
    }
  },
  SUBTASK_TUPLES_OUT_RECENT(SUBTASK_TUPLES_RECENT) {
    @Override
    protected void compute(LiebreMetricProvider provider) {
      //Computed at SUBTASK_TUPLES_RECENT
    }
  },
  SUBTASK_TUPLES_TOTAL {
    @Override
    protected void compute(LiebreMetricProvider provider) {
      SubtaskTuplesResult result = subtaskTuplesSum(provider,
          SchedulerContext.METRIC_TOTAL_PERIOD_SECONDS);
      provider.replaceMetricValues(SUBTASK_TUPLES_IN_TOTAL, result.in);
      provider.replaceMetricValues(SUBTASK_TUPLES_OUT_TOTAL, result.out);
    }
  },
  SUBTASK_TUPLES_IN_TOTAL(SUBTASK_TUPLES_TOTAL) {
    @Override
    protected void compute(LiebreMetricProvider provider) {
      //Computed at SUBTASK_TUPLES_TOTAL
    }
  },
  SUBTASK_TUPLES_OUT_TOTAL(SUBTASK_TUPLES_TOTAL) {
    @Override
    protected void compute(LiebreMetricProvider provider) {
      //Computed at SUBTASK_TUPLES_TOTAL
    }
  };


  private final Set<LiebreMetric> dependencies;

  LiebreMetric(LiebreMetric... metrics) {
    this.dependencies = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(metrics)));
  }

  @Override
  public Set<LiebreMetric> dependencies() {
    return dependencies;
  }

  protected abstract void compute(LiebreMetricProvider provider);

  protected final SubtaskTuplesResult subtaskTuplesSum(LiebreMetricProvider provider,
      int sumWindowSeconds) {
    final Map<String, Double> streamWrites = provider
        .fetchFromGraphite(movingSum(provider, "*.IN.count", sumWindowSeconds), LiebreMetricReport::last);
    final Map<String, Double> streamReads = provider
        .fetchFromGraphite(movingSum(provider, "*.OUT.count", sumWindowSeconds),
            LiebreMetricReport::last);
    final Map<String, Double> subtaskIn = new HashMap<>();
    final Map<String, Double> subtaskOut = new HashMap<>();
    for (String stream : streamWrites.keySet()) {
      String[] operators = stream.split("_");
      Validate.validState(operators.length == 2, "Invalid stream name: %s", stream);
      String writer = operators[0];
      subtaskOut.put(writer, streamWrites.get(stream));
    }
    for (String stream : streamReads.keySet()) {
      String[] operators = stream.split("_");
      Validate.validState(operators.length == 2, "Invalid stream name: %s", stream);
      String reader = operators[1];
      subtaskIn.put(reader, streamReads.get(stream));
    }
    return new SubtaskTuplesResult(subtaskIn, subtaskOut);
  }

  private static String movingSum(LiebreMetricProvider provider, String key, int sumWindowSeconds) {
    return String
        .format("movingSum(%s.%s, '%dsec')", provider.metricsPrefix, key, sumWindowSeconds);
  }

  private static final class SubtaskTuplesResult {

    final Map<String, Double> in;
    final Map<String, Double> out;

    public SubtaskTuplesResult(Map<String, Double> in,
        Map<String, Double> out) {
      this.in = Collections.unmodifiableMap(in);
      this.out = Collections.unmodifiableMap(out);
    }
  }

}
