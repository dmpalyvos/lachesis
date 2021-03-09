package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
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

/**
 * Assign all SPE processes to a single cgroup.
 */
public class OneCGroupPolicy implements CGroupPolicy {

  public static final String NAME = "ONE";
  private static final Logger LOG = LogManager.getLogger();
  private static final CGroup DEFAULT_CGROUP = CGroup.PARENT_CPU_CGROUP.newChild("all");

  @Override
  public void init(Collection<Task> tasks, SpeRuntimeInfo speRuntimeInfo,
      CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {
    translator.init();
    Map<CGroup, Collection<ExternalThread>> assignment = new HashMap<>();
    List<ExternalThread> speProcesses = new ArrayList<>();
    speRuntimeInfo.pids().forEach(pid -> speProcesses.add(new ExternalThread(pid, "SPE_PROCESS")));
    assignment.put(DEFAULT_CGROUP, speProcesses);
    LOG.info("Assigning all SPE processes ({}) to the default cgroup: {}", speRuntimeInfo.pids(), DEFAULT_CGROUP.path());
    translator.assign(assignment);
  }

  @Override
  public void apply(Collection<Task> tasks,
      SpeRuntimeInfo speRuntimeInfo, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {

  }
}
