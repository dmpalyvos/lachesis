package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.task.ExternalThread;
import java.util.Collection;
import java.util.Map;

/**
 * Translator that applies {@link CGroupPolicy} by using the linux {@code cgroup} tools.
 *
 * @see CpuQuotaCGroupTranslator
 * @see CpuSharesCGroupTranslator
 */
public interface CGroupTranslator {

  void init();

  void assign(Map<CGroup, Collection<ExternalThread>> assignment);

  void apply(Map<CGroup, Double> schedule);
}
