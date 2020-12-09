package io.palyvos.scheduler.policy;

import io.palyvos.scheduler.metric.SchedulerMetric;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.translators.cgroup.CGroupAgnosticTranslator;
import io.palyvos.scheduler.policy.translators.cgroup.CGroupSchedulingFunction;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;

public class CGroupMetricPolicy implements CGroupSchedulingPolicy {

  private final SchedulerMetric metric;
  private final CGroupAgnosticTranslator translator;
  private final CGroupSchedulingFunction scheduleFunction;

  public CGroupMetricPolicy(SchedulerMetric metric,
      CGroupAgnosticTranslator translator,
      CGroupSchedulingFunction schedulingFunction) {
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
    translator.schedule(metricProvider.get(metric), scheduleFunction);
  }
}
