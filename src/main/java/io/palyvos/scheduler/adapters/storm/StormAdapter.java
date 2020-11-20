package io.palyvos.scheduler.adapters.storm;

import io.palyvos.scheduler.adapters.OsAdapter;
import io.palyvos.scheduler.adapters.SpeAdapter;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.task.TaskIndex;
import io.palyvos.scheduler.util.QueryGraphParser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StormAdapter implements SpeAdapter {

  private static final Logger LOG = LogManager.getLogger();

  private final QueryGraphParser queryGraphParser = new QueryGraphParser();
  private final List<Task> tasks = new ArrayList<>();
  private final OsAdapter osAdapter;
  private final List<Integer> pids;
  private final String queryGraphPath;
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
  public void updateTasks() {
    tasks.clear();
    StormThreadAssigner.assign(tasks, threads());
    queryGraphParser.initTaskGraph(tasks, queryGraphPath);
    this.taskIndex = new TaskIndex(tasks);
  }

  @Override
  public Collection<Task> tasks() {
    return tasks;
  }

  @Override
  public Collection<ExternalThread> threads() {
    List<ExternalThread> threads = new ArrayList<>();
    for (int pid : pids) {
      threads.addAll(osAdapter.jvmThreads(pid));
    }
    return Collections.unmodifiableList(threads);
  }

  @Override
  public TaskIndex taskIndex() {
    return taskIndex;
  }
}
