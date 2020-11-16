package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.util.ExternalCommandRunner;
import io.palyvos.scheduler.util.ReniceCommand;
import org.apache.commons.lang3.Validate;

public class ReniceCommandTest {
  public static void main(String[] args) {
    Validate.isTrue(args.length == 2, "usage: pid niceValue");
    final int pid = Integer.valueOf(args[0]);
    final int niceValue = Integer.valueOf(args[1]);
    ReniceCommand command = new ReniceCommand(pid, niceValue);
    System.out.println(command);
    ExternalCommandRunner.run(command);
  }
}
