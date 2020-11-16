package io.palyvos.scheduler.task;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class SubtaskTest {

  @Test(expectedExceptions = {NullPointerException.class})
  void nullId() {
    new Subtask(null, "name", 0);
  }

  @Test(expectedExceptions = {NullPointerException.class})
  void nullName() {
    new Subtask("id", null, 0);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  void blankId() {
    new Subtask("", "name", 0);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  void blankName() {
    new Subtask("id", "", 0);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  void negativeIndex() {
    new Subtask("id", "name", -1);
  }

  @Test(expectedExceptions = {IllegalStateException.class})
  void reassignThread() {
    Subtask subtask = new Subtask("id", "name", 0);
    subtask.assignThread(new ExternalThread(2, "1"));
    subtask.assignThread(new ExternalThread(3, "2"));
  }

  void equality() {
    Subtask subtask1 = new Subtask("id", "name", 0);
    Subtask subtask2 = new Subtask("id", "name", 0);
    Subtask subtask3 = new Subtask("otherId", "name", 0);
    Subtask subtask4 = new Subtask("id", "name", 1);
    Assert.assertTrue(subtask1.equals(subtask2), "Equality check failed!");
    Assert.assertTrue(subtask1.hashCode() == subtask2.hashCode(), "Hashcode check failed!");
    Assert.assertTrue(!subtask1.equals(subtask3), "Inequality check failed!");
    Assert.assertTrue(!subtask1.equals(subtask4), "Inequality check failed!");
  }

}
