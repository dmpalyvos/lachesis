package io.palyvos.scheduler.adapters.storm;

import io.palyvos.scheduler.metric.Metric;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang3.Validate;

enum StormMetric implements Metric<StormMetric> {
  SUBTASK_TUPLES_IN_TOTAL {
    @Override
    protected void compute(StormMetricProvider provider) {
      Map<String, Double> metricValues = computeTuplesHelper(provider.totalSpoutInfos,
          provider.totalBoltInfos,
          si -> null, bi -> (double) bi.executed);
      provider.replaceMetricValues(this, metricValues);
      provider.historyProcessor.add(SUBTASK_TUPLES_IN_TOTAL, metricValues, provider.now);
    }
  },
  SUBTASK_TUPLES_OUT_TOTAL {
    @Override
    protected void compute(StormMetricProvider provider) {
      Map<String, Double> metricValues = computeTuplesHelper(provider.totalSpoutInfos,
          provider.totalBoltInfos,
          si -> (double) si.emitted, bi -> (double) bi.emitted);
      provider.replaceMetricValues(this, metricValues);
      provider.historyProcessor.add(SUBTASK_TUPLES_OUT_TOTAL, metricValues, provider.now);
    }
  },
  SUBTASK_TUPLES_IN_RECENT(SUBTASK_TUPLES_IN_TOTAL) {
    @Override
    protected void compute(StormMetricProvider provider) {
      Map<String, Double> metricValues = provider.historyProcessor
          .process(SUBTASK_TUPLES_IN_TOTAL, provider.now);
      provider.replaceMetricValues(this, metricValues);
    }
  },
  SUBTASK_TUPLES_OUT_RECENT(SUBTASK_TUPLES_OUT_TOTAL) {
    @Override
    protected void compute(StormMetricProvider provider) {
      Map<String, Double> metricValues = provider.historyProcessor
          .process(SUBTASK_TUPLES_OUT_TOTAL, provider.now);
      provider.replaceMetricValues(this, metricValues);

    }
  },
  //FIXME: Unused
  SUBTASK_CPU_UTILIZATION {
    @Override
    protected void compute(StormMetricProvider provider) {
      Map<String, Double> metricValues = new HashMap<>();
      provider.totalBoltInfos
          .forEach(bi -> metricValues.put(bi.boltId, Double.valueOf(bi.executeLatency)));
      provider.totalSpoutInfos
          .forEach(si -> metricValues.put(si.spoutId, Double.valueOf(si.completeLatency)));
      provider.replaceMetricValues(this, metricValues);
    }
  };

  private final Set<StormMetric> dependencies;

  StormMetric(StormMetric... metrics) {
    this.dependencies = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(metrics)));
  }

  @Override
  public Set<StormMetric> dependencies() {
    return dependencies;
  }

  protected void compute(StormMetricProvider provider) {

  }

  //FIXME: Rename/rewrite!
  protected Map<String, Double> computeTuplesHelper(Collection<StormSpoutInfo> spoutInfos,
      Collection<StormBoltInfo> boltInfos, Function<StormSpoutInfo, Double> spoutFunction,
      Function<StormBoltInfo, Double> boltFunction) {
    Map<String, Double> metricValues = new HashMap<>();
    for (StormSpoutInfo info : spoutInfos) {
      //FIXME: Hack: Dividing by #executors to prevent error due to metrics reported in subtask-basis
      Double value = spoutFunction.apply(info);
      if (value == null) {
        continue;
      }
      Object prev = metricValues.put(info.spoutId, value / info.executors);
      Validate.validState(prev == null, "Duplicate key: %s", info.spoutId);
    }
    for (StormBoltInfo info : boltInfos) {
      //FIXME: Hack: Dividing by #executors to prevent error due to metrics reported in subtask-basis
      Double value = boltFunction.apply(info);
      if (value == null) {
        continue;
      }
      Object prev = metricValues.put(info.boltId, value / info.executors);
      Validate.validState(prev == null, "Duplicate key: %s", info.boltId);
    }
    return metricValues;
  }


}
