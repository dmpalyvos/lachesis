package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.metric.BasicSchedulerMetric;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InputOutputQueuePolicy extends
    AbstractSinglePriorityPolicy {

  public static final String NAME = "TASK-INPUT-OUTPUT-QUEUE";
  private static final Logger LOG = LogManager.getLogger();
  private static final BasicSchedulerMetric INPUT_METRIC = BasicSchedulerMetric.TASK_QUEUE_SIZE_FROM_SUBTASK_DATA;
  private static final BasicSchedulerMetric OUTPUT_METRIC = BasicSchedulerMetric.TASK_OUTPUT_QUEUE_SIZE_FROM_SUBTASK_DATA;

  public InputOutputQueuePolicy() {
    super(true);
  }


  @Override
  public void init(SinglePriorityTranslator translator, SchedulerMetricProvider metricProvider) {
    metricProvider.register(INPUT_METRIC);
    metricProvider.register(OUTPUT_METRIC);
  }

  @Override
  protected Double getPriority(SchedulerMetricProvider metricProvider, Task task) {
    return getMetric(INPUT_METRIC, task, metricProvider
    );
  }

  @Override
  protected Double getHelperPriority(SchedulerMetricProvider metricProvider, Task task) {
    return getMetric(OUTPUT_METRIC, task, metricProvider);
  }

  private Double getMetric(BasicSchedulerMetric metric, Task task,
      SchedulerMetricProvider metricProvider) {
    try {
      return metricProvider.get(metric, task.id());
    } catch (Exception e) {
      LOG.error("Failed to get metric {} for task {}: {}\n",
          metric, task, e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
