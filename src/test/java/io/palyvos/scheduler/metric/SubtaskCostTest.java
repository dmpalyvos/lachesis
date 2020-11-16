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

public class SubtaskCostTest {


  public static final BaseSchedulerMetric METRIC = BaseSchedulerMetric.SUBTASK_COST;

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

  @Test(expectedExceptions = IllegalStateException.class)
  void oneTaskEmptyData() {
    final List<Task> tasks = MockTaskFactory.simpleChain(1);
    final Map<String, Double> subtaskTuplesIn = new HashMap<>();
    final Map<String, Double> subtaskTuplesOut = new HashMap<>();
    final Map<String, Double> threadCpu = new HashMap<>();
    final MockMetricProvider mockMetricProvider = new MockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider)
        .setTaskIndex(new TaskIndex(tasks));

    metricProvider.register(METRIC);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_RECENT, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_RECENT, subtaskTuplesOut);
    mockMetricProvider.replaceMetricValues(MockMetric.THREAD_CPU_UTILIZATION, threadCpu);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider.get(METRIC, task.id())).collect(Collectors.toList());
  }

  @Test
  void oneTaskSource() {
    final List<Task> tasks = MockTaskFactory.simpleChain(1);
    final Map<String, Double> subtaskTuplesIn = new HashMap<>();
    final Map<String, Double> subtaskTuplesOut = new HashMap<>();
    final Map<String, Double> threadCpu = new HashMap<>();
    final MockMetricProvider mockMetricProvider = new MockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider)
        .setTaskIndex(new TaskIndex(tasks));

    subtaskTuplesOut.put(tasks.get(0).id(), 5.0);
    threadCpu.put(String.valueOf(MockTaskFactory.START_PID+0), 30.0);
    metricProvider.register(METRIC);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_RECENT, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_RECENT, subtaskTuplesOut);
    mockMetricProvider.replaceMetricValues(MockMetric.THREAD_CPU_UTILIZATION, threadCpu);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider.get(METRIC, task.id())).collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), 30.0/(5.0));
  }

  @Test
  void oneTaskSink() {
    final List<Task> tasks = MockTaskFactory.simpleChain(1);
    final Map<String, Double> subtaskTuplesIn = new HashMap<>();
    final Map<String, Double> subtaskTuplesOut = new HashMap<>();
    final Map<String, Double> threadCpu = new HashMap<>();
    final MockMetricProvider mockMetricProvider = new MockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider)
        .setTaskIndex(new TaskIndex(tasks));

    subtaskTuplesIn.put(tasks.get(0).id(), 5.0);
    threadCpu.put(String.valueOf(MockTaskFactory.START_PID + 0), 30.0);
    metricProvider.register(METRIC);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_RECENT, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_RECENT, subtaskTuplesOut);
    mockMetricProvider.replaceMetricValues(MockMetric.THREAD_CPU_UTILIZATION, threadCpu);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider.get(METRIC, task.id())).collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), 30.0/(5.0));
  }

  @Test
  void oneTaskOperator() {
    final List<Task> tasks = MockTaskFactory.simpleChain(1);
    final Map<String, Double> subtaskTuplesIn = new HashMap<>();
    final Map<String, Double> subtaskTuplesOut = new HashMap<>();
    final Map<String, Double> threadCpu = new HashMap<>();
    final MockMetricProvider mockMetricProvider = new MockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider)
        .setTaskIndex(new TaskIndex(tasks));

    subtaskTuplesIn.put(tasks.get(0).id(), 5.0);
    subtaskTuplesOut.put(tasks.get(0).id(), 100.0);
    threadCpu.put(String.valueOf(MockTaskFactory.START_PID+0), 30.0);
    metricProvider.register(METRIC);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_RECENT, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_RECENT, subtaskTuplesOut);
    mockMetricProvider.replaceMetricValues(MockMetric.THREAD_CPU_UTILIZATION, threadCpu);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider.get(METRIC, task.id())).collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), 30.0/(5.0));
  }

  @Test
  void oneTaskOperatorZero() {
    final List<Task> tasks = MockTaskFactory.simpleChain(1);
    final Map<String, Double> subtaskTuplesIn = new HashMap<>();
    final Map<String, Double> subtaskTuplesOut = new HashMap<>();
    final Map<String, Double> threadCpu = new HashMap<>();
    final MockMetricProvider mockMetricProvider = new MockMetricProvider();
    final SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(mockMetricProvider)
        .setTaskIndex(new TaskIndex(tasks));

//    subtaskTuplesIn.put(tasks.get(0).id(), );
//    subtaskTuplesOut.put(tasks.get(0).id(), 100.0);
    threadCpu.put(String.valueOf(MockTaskFactory.START_PID+0), 30.0);
    metricProvider.register(METRIC);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_IN_RECENT, subtaskTuplesIn);
    mockMetricProvider.replaceMetricValues(MockMetric.SUBTASK_TUPLES_OUT_RECENT, subtaskTuplesOut);
    mockMetricProvider.replaceMetricValues(MockMetric.THREAD_CPU_UTILIZATION, threadCpu);
    metricProvider.run();
    List<Double> actual = tasks.stream().
        map(task -> metricProvider.get(METRIC, task.id())).collect(Collectors.toList());

    AssertHelper.assertNoNullElements(actual);
    Assert.assertEquals((double) actual.get(0), Double.NaN);
  }



}
