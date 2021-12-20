package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@Deprecated
public class DelegatingMultiSpeSinglePriorityPolicy implements
    MultiSpeSinglePriorityPolicy {

  private static final Logger LOG = LogManager.getLogger();
  private final Map<ExternalThread, Double> mergedSchedule = new HashMap<>();
  private final SinglePriorityPolicy delegate;

  public DelegatingMultiSpeSinglePriorityPolicy(SinglePriorityPolicy delegate) {
    this.delegate = delegate;
  }


  @Override
  public void init(SinglePriorityTranslator translator,
      Collection<SchedulerMetricProvider> metricProviders) {
    metricProviders.forEach(provider -> delegate.init(translator, provider));
  }

  @Override
  public void update(Collection<Task> tasks, SpeRuntimeInfo speRuntimeInfo,
      SchedulerMetricProvider metricProvider,
      double scalingFactor) {
    Map<ExternalThread, Double> schedule = delegate.computeSchedule(tasks,
        speRuntimeInfo, metricProvider);
    for (Map.Entry<ExternalThread, Double> entry : schedule.entrySet()) {
      ExternalThread key = entry.getKey();
      Double value = entry.getValue();
      Object previous = mergedSchedule.put(key, value != null ? value * scalingFactor : null);
      Validate.validState(previous == null, "Duplicate schedule key: %s (%s)", key, previous);
    }
  }

  @Override
  public void apply(SinglePriorityTranslator translator) {
    translator.apply(mergedSchedule);
    reset();
  }

  @Override
  public void reset() {
    mergedSchedule.clear();
  }
}
