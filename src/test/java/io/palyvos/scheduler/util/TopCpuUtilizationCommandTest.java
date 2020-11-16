package io.palyvos.scheduler.util;

import io.palyvos.scheduler.util.ExternalCommandRunner.CommandResult;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TopCpuUtilizationCommandTest {

  private static class MockCommand extends TopCpuUtilizationCommand {

    private static final String TOP_DATA_PATH = "src/test/resources/top-out.txt";

    public MockCommand() {
      super(2);
    }

    @Override
    public CommandResult call() {
      List<String> topData = null;
      try {
        topData = Files.newBufferedReader(Paths.get(TOP_DATA_PATH)).
            lines().collect(Collectors.toList());
        return new CommandResult(0, topData, Collections.emptyList());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  void cpuUtilization() {
    MockCommand command = new MockCommand();
    Map<String, Double> utilization = command.cpuUtilizationPerThread();
    Assert.assertTrue(!utilization.isEmpty(), "empty");
    Assert.assertTrue(utilization.get("18476") - 5.3 < 1E-2, "value 1");
    Assert.assertTrue(utilization.get("18558") - 2.6 < 1E-2, "value 2");
  }

}
