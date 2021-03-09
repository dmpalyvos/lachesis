package io.palyvos.scheduler.adapters.storm;

import io.palyvos.scheduler.adapters.OsAdapter;
import io.palyvos.scheduler.adapters.SpeAdapter;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.task.TaskIndex;
import io.palyvos.scheduler.util.QueryGraphFileParser;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StormAdapter implements SpeAdapter {

  private static final Logger LOG = LogManager.getLogger();
  public static final Function<String, String> THREAD_NAME_GRAPHITE_CONVERTER =
      s -> s.replace(".", "-");

  private final QueryGraphFileParser queryGraphFileParser = new QueryGraphFileParser();
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
    tasks.addAll(queryGraphFileParser.loadTasks(queryGraphPath, id -> new Task(id, id, "DEFAULT")));
    StormThreadAssigner.assign(tasks, threads());
    tasks.forEach(task -> task.checkHasThreads());
    final long missingTasks = tasks.stream().filter(task -> !task.hasThreads()).count();
    Validate.validState(missingTasks == 0 || (SchedulerContext.IS_DISTRIBUTED
            && (missingTasks <= SchedulerContext.MAX_REMOTE_TASKS)),
        "More remote tasks than the max allowed: %s",
        tasks.stream().filter(task -> !task.hasThreads()).collect(Collectors.toList()));
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
