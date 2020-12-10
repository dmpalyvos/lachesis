package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.util.command.ExternalCommand;
import io.palyvos.scheduler.util.command.ExternalCommandRunner.CommandResult;
import io.palyvos.scheduler.util.command.cgroup.CGClassifyCommand;
import io.palyvos.scheduler.util.command.cgroup.CGCreateCommand;
import io.palyvos.scheduler.util.command.cgroup.CGDeleteCommand;
import io.palyvos.scheduler.util.command.cgroup.CGSetCommand;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CGroup {

  private static final Logger LOG = LogManager.getLogger();

  private final String path;
  private final CGroupController[] controllers;
  private final Map<String, String> parameters = new HashMap<>();
  private final String asShortString;

  public CGroup(String path, CGroupController... controllers) {
    this.path = path;
    this.controllers = controllers;
    this.asShortString = String.format("%s:%s",
        Arrays.stream(controllers).map(c -> c.name().toLowerCase())
            .collect(Collectors.joining(",")),
        path);
  }

  public CGroup newChild(String path) {
    return new CGroup(String.format("%s/%s", this.path, path), controllers);
  }

  public boolean create() {
    CGCreateCommand command = new CGCreateCommand(path, controllers);
    return executeWithLogging(command, "create");
  }

  public boolean delete() {
    CGDeleteCommand command = new CGDeleteCommand(path, controllers);
    return executeWithLogging(command, "delete");
  }

  public boolean set(String parameter, Object value) {
    Validate.notEmpty(parameter, "empty parameter");
    Validate.notNull(value, "value");
    final String valueAsStr = String.valueOf(value);
    CGSetCommand command = new CGSetCommand(path, parameter, valueAsStr);
    if (executeWithLogging(command, "set")) {
      parameters.put(parameter, valueAsStr);
      return true;
    }
    return false;
  }

  public CGroupController[] controllers() {
    return controllers;
  }

  public String path() {
    return path;
  }

  public boolean classify(Collection<ExternalThread> threads) {
    Validate.notEmpty(threads, "no thread provided for classify");
    Collection<Integer> pids = threads.stream().map(thread -> thread.pid())
        .collect(Collectors.toList());
    CGClassifyCommand command = new CGClassifyCommand(pids, path, controllers);
    return executeWithLogging(command, "classify");
  }

  private boolean executeWithLogging(ExternalCommand command, String action) {
    CommandResult result = command.call();
    LOG.trace(command);
    if (!result.success()) {
      LOG.error("Failed to execute action {} for cgroup {}: {}", action, path,
          result.stderrAsString());
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return asShortString;
  }
}
