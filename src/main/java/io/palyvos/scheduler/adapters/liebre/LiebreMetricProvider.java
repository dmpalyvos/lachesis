package io.palyvos.scheduler.adapters.liebre;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.palyvos.scheduler.metric.AbstractMetricProvider;
import io.palyvos.scheduler.metric.BaseSchedulerMetric;
import io.palyvos.scheduler.metric.Metric;
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
  static final int GRAPHITE_FROM_TIME_WINDOW_SECONDS = 30;


  static final Map<Metric, LiebreMetric> METRIC_MAPPING = new HashMap<>();

  static {
    for (BaseSchedulerMetric metric : BaseSchedulerMetric.values()) {
      if (metric.isInternal()) {
        continue;
      }
      try {
        METRIC_MAPPING.put(metric, LiebreMetric.valueOf(metric.name()));
      } catch (Exception e) {
        LOG.trace("Metric {} does not correspond to any LiebreMetric", metric);
      }
    }
  }

  private final URI graphiteURI;
  private final Gson gson = new GsonBuilder().create();
  String metricsPrefix;

  public LiebreMetricProvider(String graphiteHost, int graphitePort, String metricsPrefix) {
    super(METRIC_MAPPING, LiebreMetric.class);
    Validate.notBlank(metricsPrefix, "metricsPrefix");
    this.graphiteURI = URI.create(String.format("http://%s:%d", graphiteHost, graphitePort));
    Validate.notBlank(metricsPrefix, "metricsPrefix");
    this.metricsPrefix = metricsPrefix;
  }

  @Override
  protected void doCompute(LiebreMetric metric) {
    metric.compute(this);
  }

  LiebreMetricReport[] rawFetchFromGraphite(String target, int fromSeconds) {
    Validate.notEmpty(target, "empty target");
    URIBuilder builder = new URIBuilder(graphiteURI);
    builder.setPath("render");
    builder.addParameter("target", target);
    builder.addParameter("from", String.format("-%dsec", fromSeconds));
    builder.addParameter("format", "json");
    try {
      URI uri = builder.build();
      LOG.trace("Fetching {}", uri);
      String response = Request.Get(uri).execute().returnContent().toString();
      LiebreMetricReport[] reports = gson.fromJson(response, LiebreMetricReport[].class);
      return reports;
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  Map<String, Double> fetchFromGraphite(String target,
      Function<LiebreMetricReport, Double> reduceFunction) {
    LiebreMetricReport[] reports = rawFetchFromGraphite(target, GRAPHITE_FROM_TIME_WINDOW_SECONDS);
    Map<String, Double> result = new HashMap<>();
    for (LiebreMetricReport report : reports) {
      Double reportValue = reduceFunction.apply(report);
      if (reportValue != null) {
        //Null values can exist due to leftovers in graphite data
        result.put(report.simpleName(), reportValue);
      }
    }
    return result;
  }

}
