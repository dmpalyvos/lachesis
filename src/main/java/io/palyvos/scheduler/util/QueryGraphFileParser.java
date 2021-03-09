package io.palyvos.scheduler.util;

import com.beust.jcommander.Strings;
import io.palyvos.scheduler.task.Task;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.yaml.snakeyaml.Yaml;

public class QueryGraphFileParser {

  private final Yaml yaml = new Yaml();

  public Collection<Task> loadTasks(String queryGraphPath, Function<String, Task> supplier) {
    Map<String, List<String>> downstreamEdges = loadQueryGraph(queryGraphPath);
    List<Task> tasks = new ArrayList<>();
    downstreamEdges.keySet().forEach(id -> tasks.add(supplier.apply(id)));
    initTaskGraph(tasks, downstreamEdges);
    return tasks;
  }

  public Collection<Task> loadTasks(String queryGraphPath) {
    return loadTasks(queryGraphPath, id -> Task.ofSingleSubtask(id));
  }

  public void initTaskGraph(Collection<Task> tasks, String queryGraphPath) {
    initTaskGraph(tasks, loadQueryGraph(queryGraphPath));
  }

  private void initTaskGraph(Collection<Task> tasks, Map<String, List<String>> downstreamEdges) {
    Map<String, Task> taskIndex = tasks.stream().collect(Collectors.toMap(t -> t.id(), t -> t));
    Validate.validState(downstreamEdges.keySet().containsAll(taskIndex.keySet()),
        "Specified query graph do not contain all SPE tasks\nSPE tasks: %s\nQuery graph tasks: %s",
        taskIndex.keySet().stream().sorted().collect(Collectors.toList()),
        downstreamEdges.keySet().stream().sorted().collect(Collectors.toList()));
    for (Task task : tasks) {
      List<String> downstream = downstreamEdges.get(task.id()) != null ?
          downstreamEdges.get(task.id()) :
          Collections.emptyList();
      for (String downstreamId : downstream) {
        Task downstreamTask = taskIndex.get(downstreamId);
        if (downstreamTask == null) {
          throw new IllegalStateException(String
              .format("Failed to initialize task graph: Undefined task '%s' referenced in an edge",
                  downstreamId));
        }
        task.downstream().add(downstreamTask);
        downstreamTask.upstream().add(task);
      }
    }
  }

  private Map<String, List<String>> loadQueryGraph(String queryGraphPath) {
    try {
      String queryGraphString = Strings.join("\n", Files.readAllLines(Paths.get(queryGraphPath)));
      Map<String, List<String>> nodes = yaml.load(queryGraphString);
      return nodes;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }
}
