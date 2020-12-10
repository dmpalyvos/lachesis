package io.palyvos.scheduler.util.command.cgroup;

import io.palyvos.scheduler.policy.cgroup.CGroupController;
import io.palyvos.scheduler.util.command.ExternalCommand;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;

public class CGClassifyCommand implements ExternalCommand {

  private static final String EXECUTABLE = "cgclassify";
  private static final String GROUPS_FLAG = "-g";

  private final String controllers;
  private final String cgroupPath;
  private final Collection<String> pids;

  public CGClassifyCommand(Collection<Integer> pids, String cgroupPath, CGroupController... controllers) {
    Validate.notEmpty(controllers, "At least one controller required!");
    Validate.notBlank(cgroupPath, "Blank cgroup path!");
    Validate.notEmpty(pids, "At least one pid required");
    this.controllers = Arrays.stream(controllers).map(c -> c.name().toLowerCase())
        .collect(Collectors.joining(","));
    this.cgroupPath = cgroupPath;
    this.pids = pids.stream().map(String::valueOf).collect(Collectors.toList());
  }

  @Override
  public List<String> rawCommand() {
    String controllersGroups = controllers + ":" + cgroupPath;
    List<String> rawCommand = new ArrayList<>();
    rawCommand.add(executable());
    rawCommand.add(GROUPS_FLAG);
    rawCommand.add(controllersGroups);
    rawCommand.addAll(pids);
    return rawCommand;
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
