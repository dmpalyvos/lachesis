package io.palyvos.scheduler.metric;

import io.palyvos.scheduler.AssertHelper;
import io.palyvos.scheduler.MockTaskFactory;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.task.TaskIndex;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SchedulerMetricProviderTest {

  private enum ExtraMockMetric implements Metric<ExtraMockMetric> {
    TASK_QUEUE_SIZE_FROM_SUBTASK_DATA;
  }

  private static class ExtraMockMetricProvider extends AbstractMetricProvider<ExtraMockMetric> {

    public ExtraMockMetricProvider() {
      super(mappingFor(ExtraMockMetric.values()), ExtraMockMetric.class);
    }

    @Override
    protected void doCompute(ExtraMockMetric metric) {
    }
  }

  @Test
  void providerPriority() {
    final List<Task> tasks = MockTaskFactory.simpleChain(1);
    final Map<String, Double> subtaskTuplesIn1 = new HashMap<>();
    final Map<String, Double> subtaskTuplesIn2 = new HashMap<>();
    final MockMetricProvider mockMetricProvider1 = new MockMetricProvider();
    final MockMetricProvider mockMetricProvider2 = new MockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider1,
        mockMetricProvider2)
        .setTaskIndex(new TaskIndex(tasks));

    subtaskTuplesIn1.put(tasks.get(0).id(), 20.0);
    subtaskTuplesIn2.put(tasks.get(0).id(), 10.0);
    metricProvider.register(SchedulerMetric.TASK_QUEUE_SIZE_FROM_SUBTASK_DATA);
    mockMetricProvider1.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_TOTAL, subtaskTuplesIn1);
    mockMetricProvider1.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_TOTAL,
        Collections.emptyMap());
    mockMetricProvider2.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_TOTAL, subtaskTuplesIn2);
    mockMetricProvider2.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_TOTAL,
        Collections.emptyMap());
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider
            .get(SchedulerMetric.TASK_QUEUE_SIZE_FROM_SUBTASK_DATA, task.id()))
        .collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), -20.0);
  }

  /**
   * Although the queue size metric is computed as in - out as defined in {@link
   * BaseCompositeMetric}, we can override it by initializing the {@link SchedulerMetricProvider}
   * with another provider that also provides queue size. By default, the {@link
   * BaseCompositeMetric}s have the lowest priority, so the custom provider "wins" instead.
   */
  @Test
  void overrideCompositeMetricProvider() {
    final List<Task> tasks = MockTaskFactory.simpleChain(1);
    final Map<String, Double> subtaskTuplesIn = new HashMap<>();
    final Map<String, Double> subtaskTuplesOut = new HashMap<>();
    final Map<String, Double> queueSize = new HashMap<>();
    final MockMetricProvider mockMetricProvider = new MockMetricProvider();
    final ExtraMockMetricProvider queueSizeMetricProvider = new ExtraMockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider,
        queueSizeMetricProvider)
        .setTaskIndex(new TaskIndex(tasks));

    subtaskTuplesIn.put(tasks.get(0).id(), 10.0);
    queueSize.put(tasks.get(0).id(), 100.0);
    metricProvider.register(SchedulerMetric.TASK_QUEUE_SIZE_FROM_SUBTASK_DATA);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_TOTAL, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_TOTAL, subtaskTuplesOut);
    queueSizeMetricProvider
        .replaceMetricValues(ExtraMockMetric.TASK_QUEUE_SIZE_FROM_SUBTASK_DATA, queueSize);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider
            .get(SchedulerMetric.TASK_QUEUE_SIZE_FROM_SUBTASK_DATA, task.id()))
        .collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), 100.0);
  }
}
