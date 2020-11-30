package io.palyvos.scheduler.adapters.flink;

import io.palyvos.scheduler.metric.AbstractMetricProvider;
import io.palyvos.scheduler.metric.graphite.GraphiteDataFetcher;
import io.palyvos.scheduler.metric.graphite.GraphiteMetricReport;
import java.util.Map;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlinkGraphiteMeticProvider extends AbstractMetricProvider<FlinkGraphiteMetric> {

  private static final Logger LOG = LogManager.getLogger();

  private final GraphiteDataFetcher graphiteDataFetcher;

  public FlinkGraphiteMeticProvider(String graphiteHost, int graphitePort) {
    super(mappingFor(FlinkGraphiteMetric.values()), FlinkGraphiteMetric.class);
    this.graphiteDataFetcher = new GraphiteDataFetcher(graphiteHost, graphitePort);
  }

  @Override
  protected void doCompute(FlinkGraphiteMetric metric) {
    metric.compute(this);
  }

  Map<String, Double> fetchFromGraphite(String target,
      int windowSeconds, Function<GraphiteMetricReport, Double> reduceFunction) {
    return graphiteDataFetcher.fetchFromGraphite(target, windowSeconds, reduceFunction);
  }

}
