package io.palyvos.scheduler.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class RealTimeThreadCommand implements ExternalCommand {

  public enum RealTimeSchedulingAlgorithm {
    FIFO("-f"),
    ROUND_ROBIN("-r");

    private final String flag;

    RealTimeSchedulingAlgorithm(String flag) {
      this.flag = flag;
    }
  }
  private static final String EXECUTABLE = "chrt";
  private static final String PRIORITY_FLAG = "-p";
  public static final long PRIORITY_MIN = 1;
  public static final long PRIORITY_MAX = 99;

  private final int pid;
  private final long priority;
  private final RealTimeSchedulingAlgorithm algorithm;

  public RealTimeThreadCommand(int pid, long priority, RealTimeSchedulingAlgorithm algorithm) {
    Validate.isTrue(pid > 1, "invalid pid: %d", pid);
    Validate.inclusiveBetween(PRIORITY_MIN, PRIORITY_MAX, priority, "Priority value must be between %d and %d",
        PRIORITY_MIN, PRIORITY_MAX);
    Validate.notNull(algorithm, "algorithm");
    this.pid = pid;
    this.priority = priority;
    this.algorithm = algorithm;
  }

  @Override
  public List<String> rawCommand() {
    List<String> rawCommand = new ArrayList<>();
    rawCommand.add(executable());
    rawCommand.add(algorithm.flag);
    rawCommand.add(PRIORITY_FLAG);
    rawCommand.add(String.valueOf(priority));
    rawCommand.add(String.valueOf(pid));
    return rawCommand;
  }

  @Override
  public String executable() {
    return EXECUTABLE;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("pid", pid)
        .append("niceValue", priority)
        .toString();
  }
}
