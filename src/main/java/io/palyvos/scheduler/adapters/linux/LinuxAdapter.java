package io.palyvos.scheduler.adapters.linux;

import io.palyvos.scheduler.adapters.OsAdapter;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.util.ExternalCommandRunner.CommandResult;
import io.palyvos.scheduler.util.JstackCommand;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Utility class that retrieves the {@link ExternalThread}s from external systems.
 */
public final class LinuxAdapter implements OsAdapter {

  private static final Pattern JVM_PID_PATTERN = Pattern
      .compile("\\s*\"(.+)\".+nid=(0x[0-9a-f]+).+");
  private static final Logger LOG = LogManager.getLogger(LinuxAdapter.class);


  @Override
  public Collection<ExternalThread> jvmThreads(int pid) {
    final List<ExternalThread> externalThreads = new ArrayList<>();
    CommandResult result = new JstackCommand(pid).call();
    if (result.exitCode != 0 || result.stdout.isEmpty()) {
      throw new IllegalStateException(String
          .format("Failed to retrieve JVM threads. Check that the provided PID is correct: %d\n%s", pid,
              result.stderrAsString()));
    }
    result.stdout.stream().forEach(line -> {
          Matcher matcher = JVM_PID_PATTERN.matcher(line);
          if (matcher.matches()) {
            externalThreads.add(new ExternalThread(Integer.decode(matcher.group(2)),
                matcher.group(1)));
          }
        }
    );
    LOG.debug("Retrieved {} threads for JVM {}", externalThreads.size(), pid);
    return externalThreads;
  }

}
