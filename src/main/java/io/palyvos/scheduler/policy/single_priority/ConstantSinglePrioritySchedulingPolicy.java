package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.HelperTask;
import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ConstantSinglePrioritySchedulingPolicy implements SinglePrioritySchedulingPolicy {

  private final double normalizedPriority;
  private final boolean scheduleHelpers;

  public ConstantSinglePrioritySchedulingPolicy(long normalizedPriority, boolean scheduleHelpers) {
    this.normalizedPriority = normalizedPriority;
    this.scheduleHelpers = scheduleHelpers;
  }

  @Override
  public void init(SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider) {
  }

  @Override
  public void apply(Collection<Task> tasks, SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider) {
    translator.apply(computeSchedule(tasks, metricProvider));
  }

  @Override
  public Map<ExternalThread, Double> computeSchedule(Collection<Task> tasks,
      SchedulerMetricProvider metricProvider) {
    final Map<ExternalThread, Double> normalizedSchedule = new HashMap<>();
    for (Task task : tasks) {
      for (Subtask subtask : task.subtasks()) {
        normalizedSchedule.put(subtask.thread(), normalizedPriority);
        if (scheduleHelpers) {
          for (HelperTask helper : task.helpers()) {
            normalizedSchedule.put(helper.thread(), normalizedPriority);
          }
        }
      }
    }
    return normalizedSchedule;
  }

}
