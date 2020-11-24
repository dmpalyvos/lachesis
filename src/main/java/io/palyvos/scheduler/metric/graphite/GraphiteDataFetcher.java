package io.palyvos.scheduler.metric.graphite;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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

public class GraphiteDataFetcher {

  private static final Logger LOG = LogManager.getLogger();
  private final URI graphiteURI;
  private final Gson gson = new GsonBuilder().create();

  public GraphiteDataFetcher(String graphiteHost, int graphitePort) {
    this.graphiteURI = URI
        .create(String.format("http://%s:%d", graphiteHost, graphitePort));
  }

  public Map<String, Double> fetchFromGraphite(String target,
      int windowSeconds, Function<GraphiteMetricReport, Double> reduceFunction) {
    GraphiteMetricReport[] reports = rawFetchFromGraphite(target, windowSeconds);
    Map<String, Double> result = new HashMap<String, Double>();
    for (GraphiteMetricReport report : reports) {
      Double reportValue = reduceFunction.apply(report);
      if (reportValue != null) {
        //Null values can exist due to leftovers in graphite data
        result.put(report.name(), reportValue);
      }
    }
    return result;
  }

  GraphiteMetricReport[] rawFetchFromGraphite(String target, int windowSeconds) {
    Validate.notEmpty(target, "empty target");
    URIBuilder builder = new URIBuilder(graphiteURI);
    builder.setPath("render");
    builder.addParameter("target", target);
    builder.addParameter("from", String.format("-%dsec", windowSeconds));
    builder.addParameter("format", "json");
    try {
      URI uri = builder.build();
      LOG.trace("Fetching {}", uri);
      String response = Request.Get(uri).execute().returnContent().toString();
      GraphiteMetricReport[] reports = gson.fromJson(response, GraphiteMetricReport[].class);
      return reports;
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}