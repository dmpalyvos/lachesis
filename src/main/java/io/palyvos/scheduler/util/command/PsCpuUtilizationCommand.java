package io.palyvos.scheduler.util.command;

import io.palyvos.scheduler.util.command.ExternalCommandRunner.CommandResult;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.Validate;

public class PsCpuUtilizationCommand implements ExternalCommand {

  private static final Pattern CPU_PER_THREAD_PATTERN = Pattern
      .compile("\\s*(\\d+)\\s*([\\d\\.]+)\\s*");
  private static final String EXECUTABLE = "ps";
  private static final String THREAD_FLAG = "-T";
  private static final String PID_FLAG = "-p";
  private static final String FORMAT_FLAG = "-o";
  private static final String FORMAT = "tid=,%cpu=";
  private static final double NO_ZERO_UTILIZATION_FIX = 0.01;
  private final int pid;


  public PsCpuUtilizationCommand(int pid) {
    Validate.isTrue(pid > 1, "invalid pid");
    this.pid = pid;
  }

  public Map<String, Double> cpuUtilizationPerThread() {
    Map<String, Double> cpuConsumption = new HashMap<>();
    CommandResult result = call();
    Validate.validState(result.exitCode == 0, "Command exited with %d", result.exitCode);
    result.stdout.forEach(line -> {
          Matcher m = CPU_PER_THREAD_PATTERN.matcher(line);
          if (m.matches()) {
            double utilization = Double.valueOf(m.group(2)) + NO_ZERO_UTILIZATION_FIX;
            cpuConsumption.put(m.group(1), utilization);
          }
        }
    );
    return cpuConsumption;

  }

  @Override
  public List<String> rawCommand() {
    return Arrays.asList(
        executable(),
        THREAD_FLAG,
        PID_FLAG,
        String.valueOf(pid),
        FORMAT_FLAG,
        FORMAT
    );
  }

  @Override
  public String executable() {
    return EXECUTABLE;
  }
}
