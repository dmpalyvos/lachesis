package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OneCGroupPolicy implements CGroupPolicy {

  public static final String NAME = "ONE";
  private static final Logger LOG = LogManager.getLogger();
  private static final CGroup DEFAULT_CGROUP = CGroup.PARENT_CPU_CGROUP.newChild("all");

  @Override
  public void init(Collection<Task> tasks, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {
    translator.init(tasks);
    Map<CGroup, Collection<ExternalThread>> assignment = new HashMap<>();
    List<ExternalThread> allThreads = new ArrayList<>();
    tasks.forEach(task -> allThreads.addAll(task.threads()));
    assignment.put(DEFAULT_CGROUP, allThreads);
    LOG.info("Assigning all tasks to the default cgroup: {}", DEFAULT_CGROUP.path());
    translator.assign(assignment);
  }

  @Override
  public void apply(Collection<Task> tasks, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {

  }
}
