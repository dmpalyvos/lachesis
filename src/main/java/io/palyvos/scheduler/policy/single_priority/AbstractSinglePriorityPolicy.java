package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.HelperTask;
import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractSinglePriorityPolicy implements
    SinglePriorityPolicy {

  protected final boolean scheduleHelpers;

  public AbstractSinglePriorityPolicy(boolean scheduleHelpers) {
    this.scheduleHelpers = scheduleHelpers;
  }

  @Override
  public void apply(Collection<Task> tasks, SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider) {
    translator.apply(computeSchedule(tasks, metricProvider));
  }

  @Override
  public Map<ExternalThread, Double> computeSchedule(Collection<Task> tasks,
      SchedulerMetricProvider metricProvider) {
    //FIXME: Ugly checks for distributed executions, need to split into task and subtask policies instead
    final Map<ExternalThread, Double> schedule = new HashMap<>();
    for (Task task : tasks) {
      if (!task.hasThreads()) {
        if (SchedulerContext.IS_DISTRIBUTED) {
          continue;
        }
        throw new IllegalStateException(
            String.format("Task %s has not threads associated with it!", task));
      }
      final double priority = getPriority(metricProvider, task);
      for (Subtask subtask : task.subtasks()) {
        if (subtask.thread() == null) {
          if (SchedulerContext.IS_DISTRIBUTED) {
            continue;
          }
          throw new IllegalStateException(
              String.format("Subtask %s has no thread associated with it!", subtask));
        }
        schedule.put(subtask.thread(), priority);
      }
      if (scheduleHelpers) {
        for (HelperTask helper : task.helpers()) {
          schedule.put(helper.thread(), priority);
        }
      }
    }
    return schedule;
  }

  protected abstract Double getPriority(SchedulerMetricProvider metricProvider, Task task);
}
