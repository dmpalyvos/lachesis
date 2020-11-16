package io.palyvos.scheduler.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class ReniceCommand implements ExternalCommand {

  private static final String EXECUTABLE = "renice";
  private static final String NICE_VALUE_FLAG = "-n";
  private static final String PID_FLAG = "-p";
  public static final long NICE_MIN_VALUE = -20;
  public static final long NICE_MAX_VALUE = 19;

  private final int pid;
  private final long niceValue;

  public ReniceCommand(int pid, long niceValue) {
    Validate.isTrue(pid > 1, "invalid pid: %d", pid);
    Validate.inclusiveBetween(NICE_MIN_VALUE, NICE_MAX_VALUE, niceValue, "Nice value must be between %d and %d",
        NICE_MIN_VALUE, NICE_MAX_VALUE);
    this.pid = pid;
    this.niceValue = niceValue;
  }

  @Override
  public List<String> rawCommand() {
    List<String> rawCommand = new ArrayList<>();
    rawCommand.add(executable());
    rawCommand.add(NICE_VALUE_FLAG);
    rawCommand.add(String.valueOf(niceValue));
    rawCommand.add(PID_FLAG);
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
        .append("niceValue", niceValue)
        .toString();
  }
}
