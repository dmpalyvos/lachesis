package io.palyvos.scheduler.adapters.flink;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.HelperTask;
import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.task.Task;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Validate;

class FlinkThreadAssigner {

  private static final String TIME_TRIGGER_THREAD_PREFIX = "Time Trigger for";
  private static final String OUTPUT_FLUSHER_THREAD_PREFIX = "OutputFlusher for";
  private static final String LEGACY_SOURCE_THREAD_PREFIX = "Legacy Source Thread -";

  public static void assign(Collection<Task> tasks, Collection<ExternalThread> threads) {
    Validate.noNullElements(tasks, "Null task in collection");
    Validate.noNullElements(threads, "Null thread in collection");
//    System.out.println("---------- SUBTASKS -----------");
//    tasks.forEach(subtask -> System.out.println(subtask.name()));
//    System.out.println("---------- THREADS ------------");
//    threads.forEach(thread -> System.out.println(thread));
//    System.out.println("---------- END ----------------");
    final Map<String, List<ExternalThread>> threadIndex = new HashMap<>();
    threads.forEach(
        thread -> threadIndex.computeIfAbsent(thread.name().trim(), (k) -> new ArrayList<>())
            .add(thread));
    for (Task task : tasks) {
      outputFlusherThreads(threadIndex, task)
          .forEach(thread -> task.helpers().add(new HelperTask(thread)));
      for (Subtask subtask : task.subtasks()) {
        executorThreads(threadIndex, subtask, task.parallelism()).forEach(
            thread -> subtask.assignThread(thread));
        legacySourceThreads(threadIndex, subtask, task.parallelism()).forEach(
            thread -> subtask.helpers().add(new HelperTask(thread)));
        timeTriggerThreads(threadIndex, subtask, task.parallelism()).forEach(
            thread -> subtask.helpers().add(new HelperTask(thread)));
      }
    }
  }

  public static Collection<ExternalThread> executorThreads(
      Map<String, List<ExternalThread>> threadIndex, Subtask subtask, int parallelism) {
    final String key = String
        .format("%s (%d/%d)", subtask.id(), 1 + subtask.index(), parallelism);
    return threadIndex.computeIfAbsent(key.trim(), (k) -> Collections.emptyList());
  }

  public static Collection<ExternalThread> legacySourceThreads(
      Map<String, List<ExternalThread>> threadIndex, Subtask subtask, int parallelism) {
    final String key = String
        .format("%s %s (%d/%d)", LEGACY_SOURCE_THREAD_PREFIX, subtask.id(), 1 + subtask.index(),
            parallelism);
    return threadIndex.computeIfAbsent(key.trim(), (k) -> Collections.emptyList());
  }

  public static Collection<ExternalThread> timeTriggerThreads(
      Map<String, List<ExternalThread>> threadIndex, Subtask subtask, int parallelism) {
    final String key = String
        .format("%s %s (%d/%d)", TIME_TRIGGER_THREAD_PREFIX, subtask.id(), 1 + subtask.index(),
            parallelism);
    return threadIndex.computeIfAbsent(key.trim(), (k) -> Collections.emptyList());
  }

  public static Collection<ExternalThread> outputFlusherThreads(
      Map<String, List<ExternalThread>> threadIndex, Task task) {
    final String key = String.format("%s %s", OUTPUT_FLUSHER_THREAD_PREFIX, task.id());
    return threadIndex.computeIfAbsent(key.trim(), (k) -> Collections.emptyList());
  }


  private FlinkThreadAssigner() {

  }

}
