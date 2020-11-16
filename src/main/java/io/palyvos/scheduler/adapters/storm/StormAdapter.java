package io.palyvos.scheduler.adapters.storm;

import com.google.gson.Gson;
import io.palyvos.scheduler.adapters.OsAdapter;
import io.palyvos.scheduler.adapters.SpeAdapter;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.task.TaskIndex;
import io.palyvos.scheduler.util.QueryGraphParser;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StormAdapter implements SpeAdapter {

  private static final Logger LOG = LogManager.getLogger();
  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 8080;
  public static final String STORM_WORKER_CLASS = "org.apache.storm.daemon.worker";

  private final QueryGraphParser queryGraphParser = new QueryGraphParser();
  private final URI stormURI;
  private final Gson gson = new Gson();
  private final List<Task> tasks = new ArrayList<>();
  private final OsAdapter osAdapter;
  private final int pid;
  private final String queryGraphPath;
  private TaskIndex taskIndex;

  public StormAdapter(int pid, String host, int port, OsAdapter osAdapter, String queryGraphPath) {
    Validate.notBlank(host, "No host provided!");
    Validate.isTrue(port > 0, "Negative port provided!");
    Validate.notNull(osAdapter, "osAdapter");
    Validate.notEmpty(queryGraphPath, "Path to query graph is empty!");
    Validate.isTrue(pid > 1, "invalid pid");
    this.stormURI = URI.create(String.format("http://%s:%d", host, port));
    this.pid = pid;
    this.osAdapter = osAdapter;
    this.queryGraphPath = queryGraphPath;
  }

  @Override
  public void updateTasks() {
    tasks.clear();
    for (StormTopologyInfo topologyInfo : topologyInfos()) {
      topologyInfo.spouts.forEach(spoutInfo ->
          tasks.add(spoutInfo.toTask(topologyInfo.id)));
      topologyInfo.bolts.forEach(boltInfo ->
          tasks.add(boltInfo.toTask(topologyInfo.id)));
    }
    StormThreadAssigner.assign(tasks, threads());
    queryGraphParser.initTaskGraph(tasks, queryGraphPath);
    this.taskIndex = new TaskIndex(tasks);
  }

  public Collection<StormTopology> topologies() {
    StormTopologySummary topologySummary = StormTopologySummary.fetch(stormURI, gson);
    LOG.debug("Storm Topologies: {}", topologySummary);
    return topologySummary.topologies;
  }

  Collection<StormTopologyInfo> topologyInfos() {
    return topologyInfos(-1);
  }

  Collection<StormTopologyInfo> topologyInfos(int windowSeconds) {
    final List<StormTopologyInfo> topologyInfos = new ArrayList<>();
    for (StormTopology topology : topologies()) {
      LOG.info("Fetching info for topology: {}", topology.id);
      StormTopologyInfo topologyInfo = StormTopologyInfo
          .fetch(topology.id, gson, stormURI, windowSeconds);
      topologyInfos.add(topologyInfo);
    }
    return topologyInfos;
  }

  @Override
  public Collection<Task> tasks() {
    return tasks;
  }

  @Override
  public Collection<ExternalThread> threads() {
    return osAdapter.jvmThreads(pid);
  }

  @Override
  public TaskIndex taskIndex() {
    return taskIndex;
  }
}
