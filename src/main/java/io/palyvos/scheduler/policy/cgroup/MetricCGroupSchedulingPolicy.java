package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.metric.SchedulerMetric;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;

public class MetricCGroupSchedulingPolicy implements CGroupSchedulingPolicy {

  private final SchedulerMetric metric;
  private final CGroupMetricTranslator translator;
  private final CGroupPriorityToParametersFunction scheduleFunction;

  public MetricCGroupSchedulingPolicy(SchedulerMetric metric,
      CGroupMetricTranslator translator,
      CGroupPriorityToParametersFunction schedulingFunction) {
    this.metric = metric;
    this.translator = translator;
    this.scheduleFunction = schedulingFunction;
  }


  @Override
  public void init(Collection<Task> tasks,
      SchedulerMetricProvider metricProvider) {
    metricProvider.register(metric);
    translator.init(tasks);
  }

  @Override
  public void apply(Collection<Task> tasks, SchedulerMetricProvider metricProvider) {
    translator.apply(metricProvider.get(metric), scheduleFunction);
  }
}
