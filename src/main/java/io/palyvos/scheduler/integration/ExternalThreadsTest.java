package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.adapters.linux.LinuxAdapter;
import java.util.Collection;
import org.apache.commons.lang3.Validate;

public class ExternalThreadsTest {

  public static void main(String args[]) {
    Validate.isTrue(args.length == 1, "usage: jvmPid");
    int jvmPid = Integer.valueOf(args[0]);
    Collection<ExternalThread> tasks = new LinuxAdapter().jvmThreads(Integer.valueOf(jvmPid));
    Validate.notEmpty(tasks, "tasks");
    tasks.forEach(task -> System.out.println(task));
  }

}
