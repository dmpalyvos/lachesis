package io.palyvos.scheduler.adapters.storm;

import com.google.gson.Gson;
import io.palyvos.scheduler.util.RequestHelper;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.http.client.utils.URIBuilder;

class StormTopologyInfo {

  public static final String TARGET_URI = "/api/v1/topology/%s";

  public String name;
  public String id;
  public long workersTotal;
  public String window;
  public String status;
  public long tasksTotal;
  public long executorsTotal;
  public String uptime;
  public long uptimeSeconds;
  public long msgTimeout;
  public String windowHint;
  public boolean schedulerDisplayResource;
  public List<StormWorkerInfo> workers = null;
  public List<StormTopologyStat> topologyStats = null;
  public List<StormSpoutInfo> spouts = null;
  public List<StormBoltInfo> bolts = null;
  public Map<String, Object> configuration = new HashMap<>();
  public long replicationCount;

  public static StormTopologyInfo fetch(String topologyId, Gson gson, URI stormURI) {
    return fetch(topologyId, gson, stormURI, -1);
  }

  public static StormTopologyInfo fetch(String topologyId, Gson gson, URI stormURI,
      int windowSeconds) {
    URIBuilder builder = new URIBuilder(stormURI);
    builder.setPath(String.format(TARGET_URI, topologyId));
    if (windowSeconds > 0) {
      builder.setParameter("window", String.valueOf(windowSeconds));
    }
    String response = RequestHelper.getContent(builder);
    StormTopologyInfo topologyInfo = gson.fromJson(response, StormTopologyInfo.class);
    return topologyInfo;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("name", name).append("id", id)
        .append("workersTotal", workersTotal).append("window", window).append("status", status)
        .append("tasksTotal", tasksTotal).append("executorsTotal", executorsTotal)
        .append("uptime", uptime).append("uptimeSeconds", uptimeSeconds)
        .append("msgTimeout", msgTimeout).append("windowHint", windowHint)
        .append("schedulerDisplayResource", schedulerDisplayResource).append("workers", workers)
        .append("topologyStats", topologyStats).append("spouts", spouts).append("bolts", bolts)
        .append("configuration", configuration).append("replicationCount", replicationCount)
        .toString();
  }
}
