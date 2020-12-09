package io.palyvos.scheduler.util.command;

import io.palyvos.scheduler.util.command.ExternalCommandRunner.CommandResult;
import java.util.ArrayList;
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

  public List<Integer> pidsFor(String javaCommand, String ignorePattern) {
    Map<String, Integer> pids = namedPids();
    List<Integer> foundPids = new ArrayList<>();
    for (String name : pids.keySet()) {
      if (name.contains(javaCommand) && !name.contains(ignorePattern)) {
        foundPids.add(pids.get(name));
      }
    }
    Validate.validState(!foundPids.isEmpty(), "Failed to retrieve pid matching pattern: %s",
        javaCommand);
    return foundPids;
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
