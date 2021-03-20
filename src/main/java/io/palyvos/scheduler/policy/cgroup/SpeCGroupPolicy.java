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
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Assign each SPE to its own group with a custom priority.
 */
public class SpeCGroupPolicy implements CGroupPolicy {

  public static final String NAME = "SPE";
  private static final Logger LOG = LogManager.getLogger();
  private static final int TOTAL_PRIORITY = 10000;
  private final Map<String, Double> weights;

  public SpeCGroupPolicy(Map<String, Double> weights) {
    Validate.notEmpty(weights, "empty weights");
    this.weights = weights;
  }

  @Override
  public void init(Collection<Task> tasks, SpeRuntimeInfo speRuntimeInfo,
      CGroupTranslator translator, SchedulerMetricProvider metricProvider) {
    translator.init();
  }

  @Override
  public void apply(Collection<Task> tasks,
      SpeRuntimeInfo speRuntimeInfo, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {
    final CGroup cgroup = CGroup.PARENT_CPU_CGROUP.newChild(speRuntimeInfo.spe());
    final Map<CGroup, Collection<ExternalThread>> assignment = new HashMap<>();
    final Map<CGroup, Double> schedule = new HashMap<>();
    Double speWeight = weights.get(speRuntimeInfo.spe());
    Validate.validState(speWeight != null, "No weight for %s", speRuntimeInfo.spe());
    schedule.put(cgroup, TOTAL_PRIORITY * speWeight);
    List<ExternalThread> speProcesses = new ArrayList<>();
    speRuntimeInfo.pids().forEach(pid -> speProcesses.add(new ExternalThread(pid, "SPE_PROCESS")));
    assignment.put(cgroup, speProcesses);
    LOG.info("Assigning SPE processes {} to cgroup {} with weight {}", speRuntimeInfo.pids(),
        cgroup.path(), speWeight);
    translator.assign(assignment);
    translator.apply(schedule);
  }
}
