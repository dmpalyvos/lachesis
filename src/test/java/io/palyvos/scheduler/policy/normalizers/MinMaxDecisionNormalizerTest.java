package io.palyvos.scheduler.policy.normalizers;

import io.palyvos.scheduler.task.Subtask;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class MinMaxDecisionNormalizerTest {

  @Test
  public void normalConstructorRange() {
    new MinMaxDecisionNormalizer(0, 1);
  }

  @Test
  public void reverseConstructorRange() {
    new MinMaxDecisionNormalizer(1, 0);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void equalConstructorRange() {
    new MinMaxDecisionNormalizer(0, 0);
  }

  @Test
  public void rangeOneValue() {
    final double oneValue = 0.1;
    MinMaxDecisionNormalizer normalizer = new MinMaxDecisionNormalizer(0, 10);
    Range r = normalizer.range(Arrays.asList(oneValue));
    Assert.assertEquals(r.min, r.max);
    Assert.assertEquals(oneValue, r.min);
  }

  @Test
  public void rangeTwoEqualValues() {
    final double oneValue = 0.1;
    MinMaxDecisionNormalizer normalizer = new MinMaxDecisionNormalizer(0, 10);
    Range r = normalizer.range(Arrays.asList(oneValue, oneValue));
    Assert.assertEquals(r.min, r.max);
    Assert.assertEquals(oneValue, r.min);
  }

  @Test
  public void rangeTwoValues() {
    final double value1 = -1000;
    final double value2 = 1424151;
    MinMaxDecisionNormalizer normalizer = new MinMaxDecisionNormalizer(0, 10);
    Range r = normalizer.range(Arrays.asList(value2, value1));
    Assert.assertEquals(value1, r.min);
    Assert.assertEquals(value2, r.max);
  }

  @Test
  public void rangeManyValues() {
    final double value1 = -1000;
    final double value2 = 1424151;
    final double value3 = 0;
    final double value4 = -5041401401041.0;
    MinMaxDecisionNormalizer normalizer = new MinMaxDecisionNormalizer(0, 10);
    Range r = normalizer.range(Arrays.asList(value2, value1, value3, value4));
    Assert.assertEquals(value4, r.min);
    Assert.assertEquals(value2, r.max);
  }

  @Test
  public void normalizeValueZeroRange() {
    int oldValue = 100;
    int newMin = 0;
    int newMax = 5;
    MinMaxDecisionNormalizer normalizer = new MinMaxDecisionNormalizer(newMin, newMax);
    Range oldRange = new Range(oldValue, oldValue);
    long normalizedValue = normalizer.normalize(oldValue, oldRange);
    Assert.assertEquals(normalizedValue, newMax);
  }

  @Test
  public void normalizeValue() {
    int oldMin = 100;
    int oldMax = 1000;
    int newMin = 0;
    int newMax = 5;
    MinMaxDecisionNormalizer normalizer = new MinMaxDecisionNormalizer(newMin, newMax);
    Range oldRange = new Range(oldMin, oldMax);
    long normalizedValue1 = normalizer.normalize(oldMin, oldRange);
    long normalizedValue2 = normalizer.normalize(oldMax, oldRange);
    Assert.assertEquals(normalizedValue1, newMin);
    Assert.assertEquals(normalizedValue2, newMax);
  }

  @Test
  public void normalizeValueReverseRange() {
    int oldMin = 100;
    int oldMax = 1000;
    int newMin = 1000;
    int newMax = -20;
    MinMaxDecisionNormalizer normalizer = new MinMaxDecisionNormalizer(newMin, newMax);
    Range oldRange = new Range(oldMin, oldMax);
    long normalizedValue1 = normalizer.normalize(oldMin, oldRange);
    long normalizedValue2 = normalizer.normalize(oldMax, oldRange);
    Assert.assertEquals(normalizedValue1, newMin);
    Assert.assertEquals(normalizedValue2, newMax);
  }

  @Test
  public void normalizeOneTask() {
    int newMin = -10;
    int newMax = 15;
    MinMaxDecisionNormalizer normalizer = new MinMaxDecisionNormalizer(newMin, newMax);
    final Map<Subtask, Double> decisions = new HashMap<>();
    Subtask task = new Subtask("1", "1", 0);
    decisions.put(task, -10.0);
    Map<Subtask, Long> newDecisions = normalizer.normalize(decisions);
    Assert.assertEquals(newDecisions.size(), decisions.size());
    Assert.assertEquals((long) newDecisions.get(task), newMax);
  }

  @Test
  public void normalizeTwoTasks() {
    int newMin = -10;
    int newMax = 15;
    MinMaxDecisionNormalizer normalizer = new MinMaxDecisionNormalizer(newMin, newMax);
    final Map<Subtask, Double> decisions = new HashMap<>();
    Subtask task1 = new Subtask("1", "1", 0);
    decisions.put(task1, -10.0);
    Subtask task2 = new Subtask("2", "2", 1);
    decisions.put(task2, 10.0);
    Map<Subtask, Long> newDecisions = normalizer.normalize(decisions);
    Assert.assertEquals(newDecisions.size(), decisions.size());
    Assert.assertEquals((long) newDecisions.get(task1), newMin);
    Assert.assertEquals((long) newDecisions.get(task2), newMax);
  }

  @Test
  public void normalizeThreeTasks() {
    int newMin = -10;
    int newMax = 15;
    MinMaxDecisionNormalizer normalizer = new MinMaxDecisionNormalizer(newMin, newMax);
    final Map<Subtask, Double> decisions = new HashMap<>();
    Subtask task1 = new Subtask("1", "1", 0);
    decisions.put(task1, -10.0);
    Subtask task2 = new Subtask("2", "2", 0);
    decisions.put(task2, 10.0);
    Subtask task3 = new Subtask("2", "3", 0);
    decisions.put(task3, 1.0);
    Map<Subtask, Long> newDecisions = normalizer.normalize(decisions);
    Assert.assertEquals(newDecisions.size(), decisions.size());
    Assert.assertEquals((long) newDecisions.get(task1), newMin);
    Assert.assertEquals((long) newDecisions.get(task2), newMax);
    Assert.assertTrue(newDecisions.get(task3) <= newMax);
    Assert.assertTrue(newMin <= newDecisions.get(task3));
  }
}
