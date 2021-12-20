package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.task.ExternalThread;
import java.util.Map;

/**
 * Translator that applies single numerical priorities to each {@link ExternalThread}.
 *
 * @see NiceSinglePriorityTranslator
 * @see RealTimeSinglePriorityTranslator
 */
public interface SinglePriorityTranslator {

  void apply(Map<ExternalThread, Double> schedule);

}
