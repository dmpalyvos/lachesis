package io.palyvos.scheduler.policy;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.translators.concrete.ConcretePolicyTranslator;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.HelperTask;
import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ConstantConcreteSchedulingPolicy implements ConcreteSchedulingPolicy {

  private final long normalizedPriority;
  private final boolean scheduleHelpers;

  public ConstantConcreteSchedulingPolicy(long normalizedPriority, boolean scheduleHelpers) {
    this.normalizedPriority = normalizedPriority;
    this.scheduleHelpers = scheduleHelpers;
  }

  @Override
  public void init(ConcretePolicyTranslator policyTranslator,
      SchedulerMetricProvider metricProvider) {
  }

  @Override
  public void apply(Collection<Task> tasks, ConcretePolicyTranslator policyTranslator,
      SchedulerMetricProvider metricProvider) {
    final Map<ExternalThread, Long> normalizedSchedule = new HashMap<>();
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
    policyTranslator.applyDirect(normalizedSchedule);
  }
}
