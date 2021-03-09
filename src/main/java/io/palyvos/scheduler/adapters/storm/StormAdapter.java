package io.palyvos.scheduler.adapters.storm;

import io.palyvos.scheduler.adapters.OsAdapter;
import io.palyvos.scheduler.adapters.SpeAdapter;
import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.task.TaskIndex;
import io.palyvos.scheduler.util.QueryGraphFileParser;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StormAdapter implements SpeAdapter {

  private static final Logger LOG = LogManager.getLogger();
  public static final Function<String, String> THREAD_NAME_GRAPHITE_CONVERTER =
      s -> s.replace(".", "-");
  public static final String SPE_NAME = "storm";

  private final QueryGraphFileParser queryGraphFileParser = new QueryGraphFileParser();
  private final OsAdapter osAdapter;
  private final String queryGraphPath;
  private final List<Integer> pids;

  private SpeRuntimeInfo speRuntimeInfo;
  private TaskIndex taskIndex;

  public StormAdapter(List<Integer> pids, OsAdapter osAdapter, String queryGraphPath) {
    Validate.notNull(osAdapter, "osAdapter");
    Validate.notEmpty(queryGraphPath, "Path to query graph is empty");
    Validate.notEmpty(pids, "At least one worker PID required");
    this.pids = pids;
    this.osAdapter = osAdapter;
    this.queryGraphPath = queryGraphPath;
  }

  @Override
  public void updateState() {
    Collection<Task> tasks = queryGraphFileParser.loadTasks(queryGraphPath, id -> new Task(id, id, "DEFAULT",
        SPE_NAME));
    List<ExternalThread> threads = osAdapter.retrieveThreads(pids);
    StormThreadAssigner.assign(tasks, threads);
    this.taskIndex = new TaskIndex(tasks);
    this.speRuntimeInfo = new SpeRuntimeInfo(pids, threads, SPE_NAME);
  }


  @Override
  public TaskIndex taskIndex() {
    return taskIndex;
  }

  @Override
  public SpeRuntimeInfo runtimeInfo() {
    return speRuntimeInfo;
  }
}
