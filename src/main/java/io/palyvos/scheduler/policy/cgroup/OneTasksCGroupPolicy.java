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
 * Assign all SPE task threads to a single cgroup.
 */
public class OneTasksCGroupPolicy implements CGroupPolicy {

  public static final String NAME = "ONE-TASKS";
  private static final Logger LOG = LogManager.getLogger();
  private final CGroup cgroup;
  private final int maxRepetitions;
  private int repetitions;

  public OneTasksCGroupPolicy(String cgroupName, int maxRepetitions) {
    Validate.notBlank(cgroupName, "blank cgroupName");
    Validate.isTrue(maxRepetitions > 0, "maxRepetitions <=0");
    this.cgroup = CGroup.PARENT_CPU_CGROUP.newChild(cgroupName);
    this.maxRepetitions = maxRepetitions;
  }

  public OneTasksCGroupPolicy() {
    this("all", 1);
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
    if (repetitions >= maxRepetitions) {
      return;
    }
    Map<CGroup, Collection<ExternalThread>> assignment = new HashMap<>();
    List<ExternalThread> allThreads = new ArrayList<>();
    tasks.forEach(task -> allThreads.addAll(task.threads()));
    assignment.put(cgroup, allThreads);
    LOG.info("Assigning all SPE task threads to the default cgroup: {}", cgroup.path());
    translator.assign(assignment);
    repetitions++;
  }
}
