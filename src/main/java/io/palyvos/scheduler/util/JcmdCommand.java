package io.palyvos.scheduler.util;

import io.palyvos.scheduler.util.ExternalCommandRunner.CommandResult;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.Validate;

public class JcmdCommand implements ExternalCommand {

  private static final Pattern JCMD_PATTERN = Pattern
      .compile("(\\d+)\\s+(.+)");
  private static final String EXECUTABLE = "jcmd";

  public Map<String, Integer> namedPids() {
    Map<String, Integer> namedPids = new HashMap<>();
    CommandResult result = call();
    Validate.validState(result.exitCode == 0, "Command exited with %d", result.exitCode);
    result.stdout.forEach(line -> {
          Matcher m = JCMD_PATTERN.matcher(line);
          if (m.matches()) {
            int pid = Integer.valueOf(m.group(1));
            String name = m.group(2).trim();
            namedPids.put(name, pid);
          }
        }
    );
    return namedPids;
  }

  public int pidFor(String javaCommand) {
    Map<String, Integer> pids = namedPids();
    for (String name : pids.keySet()) {
      if (name.contains(javaCommand)) {
        return pids.get(name);
      }
    }
    throw new IllegalStateException(String.format("Failed to retrieve pid matching pattern: %s", javaCommand));
  }

  @Override
  public List<String> rawCommand() {
    return Arrays.asList(executable());
  }

  @Override
  public String executable() {
    return EXECUTABLE;
  }
}
