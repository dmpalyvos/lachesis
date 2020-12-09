package io.palyvos.scheduler.util.command;

import io.palyvos.scheduler.util.command.ExternalCommandRunner.CommandResult;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Validate;

public class TopCpuUtilizationCommand implements ExternalCommand {

  //top -b -n 1 -H -w 512 -p 18526
  private static final String EXECUTABLE = "top";
  private static final String BATCH_MODE_FLAG = "-b";
  private static final String NUMBER_REPS_FLAG = "-n";
  private static final String NUMBER_REPS = String.valueOf(1);
  private static final String THREADS_FLAG = "-H";
  private static final String PID_FLAG = "-p";
  private static final double NO_ZERO_UTILIZATION_FIX = 0.01;
  private static final int DATA_START_LINE = 7;
  private static final int PID_COLUMN_INDEX = 0;
  private static final int CPU_UTILIZATION_COLUMN_INDEX = 8;
  private static final int NUMBER_COLUMNS = 12;

  private final int pid;


  public TopCpuUtilizationCommand(int pid) {
    Validate.isTrue(pid > 1, "invalid pid");
    this.pid = pid;
  }

  public Map<String, Double> cpuUtilizationPerThread() {
    Map<String, Double> cpuConsumption = new HashMap<>();
    CommandResult result = call();
    Validate.validState(result.exitCode == 0, "Command exited with %d", result.exitCode);
    for (int i = DATA_START_LINE; i < result.stdout.size(); i++) {
      String[] columns = result.stdout.get(i).trim().split("\\s+");
      Validate.validState(columns.length >= NUMBER_COLUMNS,
          "Incorrect number of columns returned by %s: %s", executable(), result.stdout.get(i));
      int pid = Integer.valueOf(columns[PID_COLUMN_INDEX]);
      double utilization =
          Double.valueOf(columns[CPU_UTILIZATION_COLUMN_INDEX]) + NO_ZERO_UTILIZATION_FIX;
      cpuConsumption.put(String.valueOf(pid), utilization);
    }
    return cpuConsumption;
  }

  @Override
  public List<String> rawCommand() {
    return Arrays.asList(
        executable(),
        BATCH_MODE_FLAG,
        NUMBER_REPS_FLAG,
        NUMBER_REPS,
        THREADS_FLAG,
        PID_FLAG,
        String.valueOf(pid)
    );
  }

  @Override
  public String executable() {
    return EXECUTABLE;
  }
}
