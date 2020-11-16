package io.palyvos.scheduler.util.cgroup;

import io.palyvos.scheduler.util.ExternalCommand;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class CgclassifyCommand implements ExternalCommand {

  private static final String EXECUTABLE = "cgclassify";
  private static final String GROUPS_FLAG = "-g";

  private final List<String> controllers;
  private final String cgroupPath;
  private final List<Integer> pidList;

  public CgclassifyCommand(List<CgroupController> cgroupControllers, String cgroupPath,
      List<Integer> pidList) {
    Validate.notEmpty(cgroupControllers, "At least one controller required!");
    Validate.notBlank(cgroupPath, "Blank cgroup path!");
    Validate.notEmpty(pidList, "At least one pid required");
    this.controllers = cgroupControllers.stream().map(c -> c.name().toLowerCase())
        .collect(Collectors.toList());
    this.cgroupPath = cgroupPath;
    this.pidList = pidList;
  }

  @Override
  public List<String> rawCommand() {
    String controllersGroups = String.format("%s:%s", String.join(",", controllers), cgroupPath);
    List<String> rawCommand = new ArrayList<>();
    rawCommand.add(executable());
    rawCommand.add(GROUPS_FLAG);
    rawCommand.add(controllersGroups);
    pidList.stream().forEach(pid -> rawCommand.add(String.valueOf(pid)));
    return rawCommand;
  }

  @Override
  public String executable() {
    return EXECUTABLE;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("controllers", controllers)
        .append("cgroupPath", cgroupPath)
        .append("pidList", pidList)
        .toString();
  }
}
