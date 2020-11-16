package io.palyvos.scheduler.util;

import org.testng.annotations.Test;

@Test
public class ExternalCommandRunnerTest {

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void runNullCommand() {
    ExternalCommandRunner.run();
  }

  @Test(expectedExceptions = {RuntimeException.class})
  public void runBlankCommand() {
    ExternalCommandRunner.run("");
  }

  @Test(expectedExceptions = {IllegalStateException.class})
  public void runInvalidExitCode() {
    ExternalCommandRunner.run("grep", "12314115151", "1515141231");
  }


  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void runNullCommandParts() {
    ExternalCommandRunner.run("ls", null);
  }
}
