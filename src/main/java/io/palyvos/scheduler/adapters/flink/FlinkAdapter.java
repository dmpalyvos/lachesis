package io.palyvos.scheduler.adapters.flink;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.palyvos.scheduler.adapters.OsAdapter;
import io.palyvos.scheduler.adapters.SpeAdapter;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Operator;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.task.TaskIndex;
import io.palyvos.scheduler.util.RequestHelper;
import io.palyvos.scheduler.util.SchedulerContext;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlinkAdapter implements SpeAdapter {

  private static final Logger LOG = LogManager.getLogger(FlinkAdapter.class);
  public static final int DEFAULT_FLINK_PORT = 8081;
  public static final Function<String, String> THREAD_NAME_GRAPHITE_CONVERTER = s -> s
      .replace(".", "-");

  private static final String JOBS_PATH = "jobs";
  private static final String JOBS_KEY = "jobs";
  private static final Pattern MULTIPLE_OPERATOR_PATTERN = Pattern.compile("\\s*\\((.+)\\)\\s*");
  private final URI flinkURI;
  private final Gson gson = new Gson();
  private final List<Task> tasks = new ArrayList<>();
  private final OsAdapter osAdapter;
  private final List<Integer> pids;
  private TaskIndex taskIndex;

  public FlinkAdapter(List<Integer> pids, String host, int port, OsAdapter osAdapter) {
    Validate.notBlank(host, "No host provided!");
    Validate.isTrue(port > 0, "Negative port provided!");
    Validate.notNull(osAdapter, "osAdapter");
    Validate.notEmpty(pids, "At least one worker PID required");
    this.flinkURI = URI.create(String.format("http://%s:%d", host, port));
    this.pids = pids;
    this.osAdapter = osAdapter;
  }

  @Override
  public void updateTasks() {
    this.tasks.clear();
    jobs().stream().filter(job -> job.isRunning()).forEach(job ->
        tasks.addAll(fetchTasks(job)));
    FlinkThreadAssigner.assign(tasks, threads());
    tasks.forEach(task -> updateOperators(task));
    tasks.forEach(task -> task.checkHasThreads());
    final long missingTasks = tasks.stream().filter(task -> !task.hasThreads()).count();
    Validate.validState(missingTasks == 0 || (SchedulerContext.IS_DISTRIBUTED
            && (missingTasks <= SchedulerContext.MAX_REMOTE_TASKS)),
        "More remote tasks than the max allowed: %s",
        tasks.stream().filter(task -> !task.hasThreads()).collect(Collectors.toList()));
    this.taskIndex = new TaskIndex(this.tasks);
  }

  private void updateOperators(Task task) {
    String[] chainedOperators = task.id().split("->");
    final List<String> headOperators = new ArrayList<>();
    final List<String> allOperators = new ArrayList<>();
    final List<String> tailOperators = new ArrayList<>();
    for (int i = 0; i < chainedOperators.length; i++) {
      final String operator = chainedOperators[i];
      Matcher multipleOperatorMatcher = MULTIPLE_OPERATOR_PATTERN.matcher(operator);
      if (multipleOperatorMatcher.matches()) {
        // Handle branches like SOURCE -> (FILTER1, FILTER2)
        Validate.validState(i == chainedOperators.length - 1,
            "Found branch in operators before the tail of the chain!");
        List<String> branchOperators = Arrays.asList(multipleOperatorMatcher.group(1).split(","));
        allOperators.addAll(branchOperators);
        tailOperators.addAll(branchOperators);
        continue;
      }
      if (i == 0) {
        headOperators.add(operator);
      }
      if (i == chainedOperators.length - 1) {
        tailOperators.add(operator);
      }
      allOperators.add(operator);
    }
    // Bring operator names in-sync with Flink's graphite reporter and add to Task object
    task.headOperators().addAll(namesToOperators(headOperators));
    task.operators().addAll(namesToOperators(allOperators));
    task.tailOperators().addAll(namesToOperators(tailOperators));
  }

  private List<Operator> namesToOperators(List<String> operatorNames) {
    return operatorNames.stream()
        .map(name -> name.trim().replace(" ", "-"))
        .map(name -> new Operator(name))
        .collect(Collectors.toList());
  }

  /**
   * @return The jobs of the flink cluster.
   */
  public Collection<FlinkJob> jobs() {
    URIBuilder builder = new URIBuilder(flinkURI);
    builder.setPath(JOBS_PATH);
    String response = RequestHelper.getContent(builder);
    Map<String, List<FlinkJob>> jobs = gson
        .fromJson(response, new TypeToken<Map<String, List<FlinkJob>>>() {
        }.getType());
    LOG.debug("FlinkAdapter jobs: {}", jobs);
    return jobs.get(JOBS_KEY);
  }

  @Override
  public Collection<Task> tasks() {
    return Collections.unmodifiableCollection(tasks);
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

  public Collection<Task> fetchTasks(FlinkJob job) {
    Validate.notNull(job, "job");
    return fetchTasks(job.id);
  }

  public Collection<Task> fetchTasks(String jobId) {
    Validate.notBlank(jobId, "Blank job id provided!");
    URIBuilder builder = new URIBuilder(flinkURI);
    builder.setPathSegments(JOBS_PATH, jobId);
    String response = RequestHelper.getContent(builder);
    FlinkJobInfo jobInfo = gson.fromJson(response, FlinkJobInfo.class);
    LOG.debug(jobInfo);
    return jobInfo.plan.tasks(jobInfo.vertices);
  }

}
