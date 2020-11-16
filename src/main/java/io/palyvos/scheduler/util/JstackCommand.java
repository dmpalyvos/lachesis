package io.palyvos.scheduler.util;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class JstackCommand implements ExternalCommand {

  private static final String EXECUTABLE = "jstack";
  private final int jvmPid;

  public JstackCommand(int jvmPid) {
    Validate.isTrue(jvmPid > 1, "invalid pid: %d", jvmPid);
    this.jvmPid = jvmPid;
  }

  @Override
  public List<String> rawCommand() {
    return Arrays.asList(executable(), String.valueOf(jvmPid));
  }

  @Override
  public String executable() {
    return EXECUTABLE;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("jvmPid", jvmPid)
        .toString();
  }
}
