package io.palyvos.scheduler.util.cgroup;

import io.palyvos.scheduler.util.ExternalCommand;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;

public class CGDeleteCommand implements ExternalCommand {

  private static final String EXECUTABLE = "cgdelete";
  private static final String RECURSIVE_FLAG = "-r";

  private final String cgroupPath;
  private final String controllers;

  public CGDeleteCommand(String cgroupPath, CGController... controllers) {
    Validate.notBlank(cgroupPath, "blank cgroup path");
    Validate.notEmpty(controllers, "no cgroup controller specified");
    this.cgroupPath = cgroupPath;
    this.controllers = Arrays.stream(controllers).map(c -> c.name().toLowerCase())
        .collect(Collectors.joining(","));
  }

  @Override
  public List<String> rawCommand() {
    String controllersGroups = String.format("%s:%s", controllers, cgroupPath);
    return Arrays.asList(executable(), RECURSIVE_FLAG, controllersGroups);
  }

  @Override
  public String executable() {
    return EXECUTABLE;
  }

  @Override
  public String toString() {
    return rawCommandAsString();
  }
}
