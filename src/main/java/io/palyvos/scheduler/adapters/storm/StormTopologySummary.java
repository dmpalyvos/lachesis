package io.palyvos.scheduler.adapters.storm;

import com.google.gson.Gson;
import io.palyvos.scheduler.util.RequestHelper;
import java.util.List;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.http.client.utils.URIBuilder;

class StormTopologySummary {

  public static final String TARGET_URI = "api/v1/topology/summary";
  public List<StormTopology> topologies = null;
  public Boolean schedulerDisplayResource;


  public static StormTopologySummary fetch(java.net.URI stormURI, Gson gson) {
    URIBuilder builder = new URIBuilder(stormURI);
    builder.setPath(StormTopologySummary.TARGET_URI);
    String response = RequestHelper.getContent(builder);
    StormTopologySummary topologySummary = gson.fromJson(response, StormTopologySummary.class);
    return topologySummary;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("topologies", topologies)
        .append("schedulerDisplayResource", schedulerDisplayResource)
        .toString();
  }
}
