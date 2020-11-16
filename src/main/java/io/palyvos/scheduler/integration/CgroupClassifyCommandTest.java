package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.util.ExternalCommandRunner;
import io.palyvos.scheduler.util.cgroup.CgclassifyCommand;
import io.palyvos.scheduler.util.cgroup.CgroupController;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.Validate;

public class CgroupClassifyCommandTest {

  public static void main(String[] args) {
    Validate.isTrue(args.length >= 2, "usage: cgroupPath pidList");
    final String cgroupPath = args[0];
    List<Integer> pidList = new ArrayList<>();
    for (int i = 1; i < args.length; i++) {
      pidList.add(Integer.valueOf(args[i]));
    }
    CgclassifyCommand command = new CgclassifyCommand(Arrays.asList(CgroupController.CPU), cgroupPath,
        pidList);
    System.out.println(command);
    ExternalCommandRunner.run(command);
  }

}
