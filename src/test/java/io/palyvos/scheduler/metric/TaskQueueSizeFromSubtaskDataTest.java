package io.palyvos.scheduler.metric;

import io.palyvos.scheduler.AssertHelper;
import io.palyvos.scheduler.MockTaskFactory;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.task.TaskIndex;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test for {@link BaseSchedulerMetric#TASK_QUEUE_SIZE_FROM_SUBTASK_DATA}. Task and subtask IDs are
 * equal in this test.
 */
@Test
public class TaskQueueSizeFromSubtaskDataTest {

  public static final BaseSchedulerMetric METRIC = BaseSchedulerMetric.TASK_QUEUE_SIZE_FROM_SUBTASK_DATA;

  @Test(expectedExceptions = {IllegalStateException.class})
  void noData() {
    final List<Task> tasks = MockTaskFactory.simpleChain(1);
    final MockMetricProvider mockMetricProvider = new MockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider)
        .setTaskIndex(new TaskIndex(tasks));

    metricProvider.register(METRIC);
    metricProvider.run();
    metricProvider.get(METRIC, tasks.get(0).id());
  }

  @Test
  void oneTaskEmptyData() {
    final List<Task> tasks = MockTaskFactory.simpleChain(1);
    final Map<String, Double> subtaskTuplesIn = new HashMap<>();
    final Map<String, Double> subtaskTuplesOut = new HashMap<>();
    final MockMetricProvider mockMetricProvider = new MockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider)
        .setTaskIndex(new TaskIndex(tasks));

    metricProvider.register(METRIC);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_TOTAL, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_TOTAL, subtaskTuplesOut);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider.get(METRIC, task.id())).collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), 0.0);
  }



  @Test
  void oneTask() {
    final List<Task> tasks = MockTaskFactory.simpleChain(1);
    final Map<String, Double> subtaskTuplesIn = new HashMap<>();
    final Map<String, Double> subtaskTuplesOut = new HashMap<>();
    final MockMetricProvider mockMetricProvider = new MockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider)
        .setTaskIndex(new TaskIndex(tasks));

    subtaskTuplesIn.put(tasks.get(0).id(), 10.0);
    metricProvider.register(METRIC);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_TOTAL, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_TOTAL, subtaskTuplesOut);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider.get(METRIC, task.id())).collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), -10.0);
  }

  @Test
  void twoTasksEmptyData() {
    final List<Task> tasks = MockTaskFactory.simpleChain(2);

    final Map<String, Double> subtaskTuplesIn = new HashMap<>();
    final Map<String, Double> subtaskTuplesOut = new HashMap<>();
    final MockMetricProvider mockMetricProvider = new MockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider)
        .setTaskIndex(new TaskIndex(tasks));

    metricProvider.register(METRIC);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_TOTAL, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_TOTAL, subtaskTuplesOut);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider.get(METRIC, task.id())).collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), 0.0);
    Assert.assertEquals((double) actual.get(1), 0.0);
  }

  @Test
  void twoTasks() {
    final List<Task> tasks = MockTaskFactory.simpleChain(2);
    final Map<String, Double> subtaskTuplesIn = new HashMap<>();
    final Map<String, Double> subtaskTuplesOut = new HashMap<>();
    final MockMetricProvider mockMetricProvider = new MockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider)
        .setTaskIndex(new TaskIndex(tasks));
    subtaskTuplesOut.put(tasks.get(0).id(), 100.0);
    subtaskTuplesIn.put(tasks.get(1).id(), 50.0);

    metricProvider.register(METRIC);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_TOTAL, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_TOTAL, subtaskTuplesOut);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider.get(METRIC, task.id())).collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), 0.0, "1");
    Assert.assertEquals((double) actual.get(1), 50.0, "2");
  }

}
