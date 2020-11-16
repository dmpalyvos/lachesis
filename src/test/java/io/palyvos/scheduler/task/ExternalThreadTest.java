package io.palyvos.scheduler.task;

import org.testng.annotations.Test;

@Test
public class ExternalThreadTest {

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void negativePid() {
    new ExternalThread(-1, "test");
  }

  @Test(expectedExceptions = {NullPointerException.class})
  public void nullName() {
    new ExternalThread(5, null);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void emptyName() {
    new ExternalThread(5, "");
  }
}
