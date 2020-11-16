package io.palyvos.scheduler.util;

import io.palyvos.scheduler.util.ExternalCommandRunner.CommandResult;
import java.util.List;
import java.util.concurrent.Callable;

public interface ExternalCommand extends Runnable, Callable<CommandResult> {

  List<String> rawCommand();

  String executable();

  default String rawCommandAsString() {
    return String.join(" ", rawCommand());
  }

  @Override
  default void run() {
    ExternalCommandRunner.run(rawCommand());
  }

  default CommandResult call() {
    return ExternalCommandRunner.run(rawCommand(), true, true);
  }
}
