package io.palyvos.scheduler.adapters.liebre;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.palyvos.scheduler.metric.AbstractMetricProvider;
import io.palyvos.scheduler.metric.SchedulerMetric;
import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.metric.graphite.GraphiteDataFetcher;
import io.palyvos.scheduler.metric.graphite.GraphiteMetricReport;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.lang3.Validate;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//FIXME: Builder
public class LiebreMetricProvider extends AbstractMetricProvider<LiebreMetric> {

  private static final Logger LOG = LogManager.getLogger(LiebreMetricProvider.class);

  private final GraphiteDataFetcher graphiteDataFetcher;
  final String metricsPrefix;

  public LiebreMetricProvider(String graphiteHost, int graphitePort, String metricsPrefix) {
    super(mappingFor(LiebreMetric.values()), LiebreMetric.class);
    this.graphiteDataFetcher = new GraphiteDataFetcher(graphiteHost, graphitePort);
    Validate.notBlank(metricsPrefix, "metricsPrefix");
    this.metricsPrefix = metricsPrefix;
  }

  @Override
  protected void doCompute(LiebreMetric metric) {
    metric.compute(this);
  }

  public Map<String, Double> fetchFromGraphite(String target, int windowSeconds,
      Function<GraphiteMetricReport, Double> reduceFunction) {
    return graphiteDataFetcher.fetchFromGraphite(target, windowSeconds, reduceFunction);
  }
}
