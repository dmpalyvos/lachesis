package io.palyvos.scheduler.task;

import io.palyvos.scheduler.util.ExternalCommand;
import io.palyvos.scheduler.util.ExternalCommandRunner.CommandResult;
import io.palyvos.scheduler.util.cgroup.CGClassifyCommand;
import io.palyvos.scheduler.util.cgroup.CGController;
import io.palyvos.scheduler.util.cgroup.CGCreateCommand;
import io.palyvos.scheduler.util.cgroup.CGDeleteCommand;
import io.palyvos.scheduler.util.cgroup.CGSetCommand;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CGroup {

  private static final Logger LOG = LogManager.getLogger();

  private final String path;
  private final CGController[] controllers;
  private final Map<String, String> parameters = new HashMap<>();

  public CGroup(String path, CGController... controllers) {
    this.path = path;
    this.controllers = controllers;
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

  public CGController[] controllers() {
    return controllers;
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
    return new ToStringBuilder(this)
        .append("path", path)
        .append("controllers", controllers)
        .append("parameters", parameters)
        .toString();
  }
}
