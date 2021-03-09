package io.palyvos.scheduler.task;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test
public class TaskTest {

  private static final String DEFAULT_SPE = "default";


  @DataProvider(name = "scheduler-task-constructor")
  public Object[][] constructorProvider() {
    return new Object[][]{
        {null, "name", "job", NullPointerException.class},
        {"id", null, "job", NullPointerException.class},
        {"id", "name", null, NullPointerException.class},
        {"", "name", "job", IllegalArgumentException.class},
        {"id", "", "job", IllegalArgumentException.class},
        {"id", "name", "", IllegalArgumentException.class},
    };
  }

  @Test(dataProvider = "scheduler-task-constructor")
  public void invalidConstruction(String id, String name, String jobId,
      Class<? extends Exception> expectedException) {
    Assert.assertNotNull(expectedException);
    try {
      new Task(id, name, jobId, DEFAULT_SPE);
    } catch (Exception e) {
      Assert.assertTrue(expectedException.isInstance(e));
    }
  }

  @Test
  public void validConstruction() {
    new Task("id", "name", "job", DEFAULT_SPE);
  }

  @Test
  public void equality() {
    Task task1 = new Task("id1", "name1", "job", DEFAULT_SPE);
    Task task2 = new Task("id1", "name1", "job", DEFAULT_SPE);
    Assert.assertTrue(task1.equals(task2), "Equality check failed");
    Assert.assertTrue(task1.hashCode() == task2.hashCode(), "HashCode check failed!");
    Task task3 = new Task("id2", "name1", "job", DEFAULT_SPE);
    Assert.assertTrue(!task1.equals(task3), "Inequality check failed!");
  }
}
