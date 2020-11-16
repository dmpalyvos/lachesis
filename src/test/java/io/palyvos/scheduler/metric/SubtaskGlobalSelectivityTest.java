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

@Test
public class SubtaskGlobalSelectivityTest {


  public static final BaseSchedulerMetric METRIC = BaseSchedulerMetric.SUBTASK_GLOBAL_SELECTIVITY;

  @Test(expectedExceptions = {IllegalStateException.class})
  void noData() {
    final List<Task> tasks = MockTaskFactory.simpleChain(1);
    final MockMetricProvider mockMetricProvider = new MockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider)
        .setTaskIndex(new TaskIndex(tasks));

    metricProvider.register(METRIC);
    metricProvider.run();
    metricProvider.get(METRIC);
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
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_RECENT, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_RECENT, subtaskTuplesOut);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider.get(METRIC, task.id())).collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), 1.0);
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
    subtaskTuplesOut.put(tasks.get(0).id(), 5.0);
    metricProvider.register(METRIC);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_RECENT, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_RECENT, subtaskTuplesOut);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider.get(METRIC, task.id())).collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), 5.0/10.0);
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
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_RECENT, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_RECENT, subtaskTuplesOut);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider.get(METRIC, task.id())).collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), 1.0);
    Assert.assertEquals((double) actual.get(1), 1.0);
  }

  @Test
  void twoTasksPartialData() {
    final List<Task> tasks = MockTaskFactory.simpleChain(2);

    final Map<String, Double> subtaskTuplesIn = new HashMap<>();
    final Map<String, Double> subtaskTuplesOut = new HashMap<>();
    final MockMetricProvider mockMetricProvider = new MockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider)
        .setTaskIndex(new TaskIndex(tasks));

    subtaskTuplesOut.put(tasks.get(0).id(), 10.0); // Source
    subtaskTuplesIn.put(tasks.get(1).id(), 5.0); // Sink
    metricProvider.register(METRIC);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_RECENT, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_RECENT, subtaskTuplesOut);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider.get(METRIC, task.id())).collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), 1.0);
    Assert.assertEquals((double) actual.get(1), 1.0);
  }

  @Test
  void threeTasks() {
    final List<Task> tasks = MockTaskFactory.simpleChain(3);

    final Map<String, Double> subtaskTuplesIn = new HashMap<>();
    final Map<String, Double> subtaskTuplesOut = new HashMap<>();
    final MockMetricProvider mockMetricProvider = new MockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider)
        .setTaskIndex(new TaskIndex(tasks));

    // Source
    subtaskTuplesOut.put(tasks.get(0).id(), 10.0);
    // Operator
    subtaskTuplesIn.put(tasks.get(1).id(), 10.0);
    subtaskTuplesOut.put(tasks.get(1).id(), 5.0);
    // Sink
    subtaskTuplesIn.put(tasks.get(2).id(), 5.0);
    metricProvider.register(METRIC);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_RECENT, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_RECENT, subtaskTuplesOut);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider.get(METRIC, task.id())).collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), 1.0 * (5.0/10.0), "1");
    Assert.assertEquals((double) actual.get(1), 5.0/10.0, "2");
    Assert.assertEquals((double) actual.get(2), 1.0, "3");
  }

  @Test
  void fourTasks() {
    final List<Task> tasks = MockTaskFactory.simpleChain(4);

    final Map<String, Double> subtaskTuplesIn = new HashMap<>();
    final Map<String, Double> subtaskTuplesOut = new HashMap<>();
    final MockMetricProvider mockMetricProvider = new MockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider)
        .setTaskIndex(new TaskIndex(tasks));

    // Source
    subtaskTuplesOut.put(tasks.get(0).id(), 10.0);
    // Operator 1
    subtaskTuplesIn.put(tasks.get(1).id(), 10.0);
    subtaskTuplesOut.put(tasks.get(1).id(), 5.0);
    // Operator 2
    subtaskTuplesIn.put(tasks.get(2).id(), 5.0);
    subtaskTuplesOut.put(tasks.get(2).id(), 2.0);
    // Sink
    subtaskTuplesIn.put(tasks.get(3).id(), 2.0);
    metricProvider.register(METRIC);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_RECENT, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_RECENT, subtaskTuplesOut);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider.get(METRIC, task.id())).collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), 1.0 * (5.0/10.0) * (2.0/5.0), "1");
    Assert.assertEquals((double) actual.get(1), (5.0/10.0) * (2.0/5.0), "2");
    Assert.assertEquals((double) actual.get(2), 2.0 / 5.0, "3");
    Assert.assertEquals((double) actual.get(3), 1.0, "4");
  }

  //TODO: Multiple subtasks


}
