package io.palyvos.scheduler.policy.translators.concrete;

import io.palyvos.scheduler.policy.translators.concrete.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.policy.translators.concrete.normalizers.MinMaxDecisionNormalizer;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.util.SchedulerContext;
import io.palyvos.scheduler.util.cgroup.CgclassifyCommand;
import io.palyvos.scheduler.util.cgroup.CgroupController;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CgroupSplitTranslator implements ConcretePolicyTranslator {

  private static final Logger LOG = LogManager.getLogger(CgroupSplitTranslator.class);
  private final String[] cgroups;
  private final DecisionNormalizer normalizer;

  public CgroupSplitTranslator(String... cgroups) {
    Validate.notEmpty(cgroups, "No cgroups provided!");
    this.cgroups = cgroups;
    this.normalizer = new MinMaxDecisionNormalizer(0, cgroups.length - 1);
  }

  @Override
  public void applyPolicy(Map<ExternalThread, Double> schedule) {
    final Map<ExternalThread, Long> normalizedSchedule = normalizer.normalize(schedule);
    applyDirect(normalizedSchedule);
  }

  @Override
  public int applyDirect(Map<ExternalThread, Long> normalizedSchedule) {
    final Map<String, List<ExternalThread>> classification = new HashMap<>();
    Arrays.stream(cgroups).forEach(cgroup -> classification.put(cgroup, new ArrayList<>()));
    for (Map.Entry<ExternalThread, Long> decision : normalizedSchedule.entrySet()) {
      ExternalThread thread = decision.getKey();
      // Just in case there is an overflow, although impossible under normal circumstances
      // since max value is based on cgroup array length, which is an int
      int cgroupIndex = Math.toIntExact(decision.getValue());
      String cgroup = cgroups[cgroupIndex];
      classification.get(cgroup).add(thread);
    }
    SchedulerContext.switchToRootContext();
    for (Map.Entry<String, List<ExternalThread>> cgroupEntry : classification.entrySet()) {
      LOG.info("{} -> [{}]", cgroupEntry.getKey(),
          String.join(",",
              cgroupEntry.getValue()
                  .stream().map(thread -> thread.name()).collect(Collectors.toList())));
      new CgclassifyCommand(Arrays.asList(CgroupController.CPU), cgroupEntry.getKey(),
          cgroupEntry.getValue().stream().map(thread -> thread.pid())
              .collect(Collectors.toList())).run();
    }
    SchedulerContext.switchToSpeProcessContext();
    return 0; //FIXME
  }
}
