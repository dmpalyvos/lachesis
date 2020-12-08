package io.palyvos.scheduler.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//FIXME: Convert to regular class to facilitate testing
@Deprecated
public class ExternalCommandRunner {

  private static final Logger LOG = LogManager.getLogger(ExternalCommandRunner.class);

  public static void run(ExternalCommand command) {
    Validate.notNull(command, "command");
    run(command.rawCommand());
  }

  public static void run(String... command) {
    run(Arrays.asList(command));
  }

  public static void run(List<String> command) {
    final CommandResult result = ExternalCommandRunner.run(command, false, false);
    LOG.trace("{} returned: {}", commandString(command), result);
    Validate.validState(result.exitCode == 0, "Command returned exit code %d: %s", result.exitCode,
        commandString(command));
  }

  public static CommandResult run(ExternalCommand command, boolean captureStdout,
      boolean captureStderr) {
    Validate.notNull(command, "command");
    return run(command.rawCommand(), captureStdout, captureStderr);
  }

  public static CommandResult run(List<String> command, boolean captureStdout,
      boolean captureStderr) {
    Process process = startProcess(command);
    List<String> stdout = Collections.emptyList();
    List<String> stderr = Collections.emptyList();
    if (captureStdout) {
      stdout = inputStreamToList(process.getInputStream());
    }
    if (captureStderr) {
      stderr = inputStreamToList(process.getErrorStream());
    }
    try {
      final int exitCode = process.waitFor();
      process.destroy();
      return new CommandResult(exitCode, stdout, stderr);
    } catch (InterruptedException e) {
      LOG.error("Interrupted while executing: {}", commandString(command));
      throw new RuntimeException(e);
    }
  }

  private static List<String> inputStreamToList(InputStream inputStream) {
    final List<String> lines = new ArrayList<>();
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    reader.lines().forEach(line -> lines.add(line));
    return lines;
  }

  private static Process startProcess(List<String> command) {
    Validate.notEmpty(command, "Empty command!");
    Validate.noNullElements(command, "Null elements in command!");
    Validate.notBlank(command.get(0), "No executable provided!");
    //TODO: Remove for final version to reduce performance overhead
    LOG.trace("Executing command: {}", commandString(command));
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.command(command);
    try {
      return processBuilder.start();
    } catch (IOException e) {
      LOG.error("Error executing: {}", commandString(command));
      throw new RuntimeException(e);
    }
  }

  private static String commandString(List<String> command) {
    return String.join(" ", command);
  }


  public static class CommandResult {

    public final int exitCode;
    public final List<String> stdout;
    public final List<String> stderr;

    public CommandResult(int exitCode, List<String> stdout, List<String> stderr) {
      Validate.notNull(stdout, "stdout");
      Validate.notNull(stderr, "stderr");
      this.exitCode = exitCode;
      this.stdout = stdout;
      this.stderr = stderr;
    }

    public String stdoutAsString() {
      return String.join("\n", stdout);
    }

    public String stderrAsString() {
      return String.join("\n", stderr);
    }

    public boolean success() {
      return exitCode == 0;
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this)
          .append("exitCode", exitCode)
          .append("stdout", stdout)
          .append("stderr", stderr)
          .toString();
    }
  }

}
