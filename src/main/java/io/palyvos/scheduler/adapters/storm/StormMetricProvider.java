package io.palyvos.scheduler.adapters.storm;

import io.palyvos.scheduler.metric.AbstractMetricProvider;
import io.palyvos.scheduler.metric.BaseSchedulerMetric;
import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.metric.MetricHistoryProcessor;
import io.palyvos.scheduler.metric.MetricHistoryProcessor.WindowLatestMinusEarliestFunction;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StormMetricProvider extends AbstractMetricProvider<StormMetric> {

  private static final Logger LOG = LogManager.getLogger();

  static final Map<Metric, StormMetric> METRIC_MAPPING = new HashMap<>();
  private static final BiFunction<Double, Double, Double> METRIC_MERGE_FUNCTION = (prev, curr) ->
      curr - prev;

  static {
    for (BaseSchedulerMetric metric : BaseSchedulerMetric.values()) {
      if (metric.isInternal()) {
        continue;
      }
      try {
        METRIC_MAPPING.put(metric, StormMetric.valueOf(metric.name()));
      } catch (Exception e) {
        LOG.trace("Metric {} does not correspond to any StormMetric", metric);
      }
    }
  }

  private final StormAdapter stormAdapter;
  final List<StormSpoutInfo> totalSpoutInfos = new ArrayList<>();
  final List<StormBoltInfo> totalBoltInfos = new ArrayList<>();
  final MetricHistoryProcessor<StormMetric> historyProcessor = new MetricHistoryProcessor<>(
      TimeUnit.SECONDS.toMillis(SchedulerContext.METRIC_RECENT_PERIOD_SECONDS),
      WindowLatestMinusEarliestFunction.INSTANCE, StormMetric.SUBTASK_TUPLES_IN_TOTAL,
      StormMetric.SUBTASK_TUPLES_OUT_TOTAL
  );
  long now = -1;


  public StormMetricProvider(StormAdapter stormAdapter) {
    super(METRIC_MAPPING, StormMetric.class);
    Validate.notNull(stormAdapter, "stormAdapter");
    this.stormAdapter = stormAdapter;
  }

  @Override
  public void reset() {
    super.reset();
    now = System.currentTimeMillis();
    historyProcessor.cleanup(now);
    fetchComponentMetrics(-1, totalSpoutInfos, totalBoltInfos);
  }

  private void fetchComponentMetrics(int windowSeconds, List<StormSpoutInfo> spoutInfo,
      List<StormBoltInfo> boltInfo) {
    spoutInfo.clear();
    boltInfo.clear();
    for (StormTopologyInfo topologyInfo : stormAdapter.topologyInfos(windowSeconds)) {
      topologyInfo.spouts.forEach(spout -> spoutInfo.add(spout));
      topologyInfo.bolts.forEach(bolt -> boltInfo.add(bolt));
    }
  }

  @Override
  protected void doCompute(StormMetric metric) {
    metric.compute(this);
  }

}
